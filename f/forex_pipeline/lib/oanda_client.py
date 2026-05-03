import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

GRANULARITY_SECONDS: dict[str, int] = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H2": 7200, "H4": 14400, "D": 86400,
}

MAX_CANDLES_PER_REQUEST: int = 5000


def _format_oanda_timestamp(dt: datetime) -> str:
    """Format a UTC datetime to OANDA's RFC3339 nanosecond format."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


class RateLimitError(Exception):
    pass


class OandaClient:

    def __init__(self, api_token: str, account_id: str, base_url: str) -> None:
        self._account_id = account_id
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

    @retry(
        retry=retry_if_exception_type(
            (requests.Timeout, requests.ConnectionError, RateLimitError)
        ),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _get(self, url: str, params: dict) -> dict:
        response = requests.get(
            url, params=params, headers=self._headers, timeout=10)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 2))
            log.warning("Rate limit hit — retry after %ds", retry_after)
            raise RateLimitError(f"Rate limited — retry after {retry_after}s")
        response.raise_for_status()
        return response.json()

    def get_candles_range(
        self,
        instrument: str,
        granularity: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Paginate OANDA candles endpoint and return a single DataFrame.

        Only complete candles (complete=True) are included — candles still
        forming in real time are explicitly excluded at the API response level.
        Returns an empty DataFrame if no complete candles exist in the range.
        Raises on HTTP errors or unsupported granularity.
        """
        if granularity not in GRANULARITY_SECONDS:
            raise ValueError(f"Unsupported granularity: {granularity!r}")

        url = f"{self._base_url}/v3/instruments/{instrument}/candles"
        step = timedelta(seconds=GRANULARITY_SECONDS[granularity])
        current_from = from_dt
        page_dfs: list[pd.DataFrame] = []

        log.info("Fetching %s %s | %s → %s", instrument, granularity, from_dt, to_dt)

        while current_from < to_dt:
            params = {
                "granularity": granularity,
                "from": _format_oanda_timestamp(current_from),
                "count": MAX_CANDLES_PER_REQUEST,
                "price": "M",
            }
            data = self._get(url, params)
            candles = data.get("candles", [])

            if not candles:
                break

            # Filter to complete candles only — excludes the still-forming candle
            complete = [c for c in candles if c.get("complete", False)]
            if complete:
                page_dfs.append(self._parse_candles(complete, instrument, granularity))

            # Advance cursor using the last candle of the full page (including incomplete)
            # so the next page starts immediately after without overlap or gap
            page_last_time = pd.to_datetime(candles[-1]["time"], utc=True)
            if page_last_time >= to_dt or len(candles) < MAX_CANDLES_PER_REQUEST:
                break
            current_from = page_last_time.to_pydatetime() + step

        df = pd.concat(page_dfs, ignore_index=True) if page_dfs else pd.DataFrame()
        log.info("Fetched %d complete candles for %s %s", len(df), instrument, granularity)
        return df

    @staticmethod
    def _parse_candles(
        candles: list[dict], instrument: str, granularity: str
    ) -> pd.DataFrame:
        records = []
        for c in candles:
            mid = c.get("mid", {})
            records.append({
                "time":        c["time"],
                "instrument":  instrument,
                "granularity": granularity,
                "open":        float(mid["o"]) if "o" in mid else None,
                "high":        float(mid["h"]) if "h" in mid else None,
                "low":         float(mid["l"]) if "l" in mid else None,
                "close":       float(mid["c"]) if "c" in mid else None,
                "volume":      int(c.get("volume", 0)),
            })
        df = pd.DataFrame(records)
        if df.empty:
            return df
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df.sort_values("time").reset_index(drop=True)
