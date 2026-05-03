import logging

import pandas as pd

log = logging.getLogger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "time", "instrument", "granularity",
    "open", "high", "low", "close", "volume",
]


def validate_candles(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean a candle DataFrame. Returns the cleaned frame.

    Drops rows with nulls in required columns.
    Drops rows with invalid OHLC relationships.
    Raises ValueError if required columns are missing entirely.
    """
    if df.empty:
        return df

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    rows_in = len(df)

    # Drop nulls in any required column
    df = df.dropna(subset=REQUIRED_COLUMNS).copy()
    dropped_nulls = rows_in - len(df)
    if dropped_nulls:
        log.warning("Dropped %d rows with null values", dropped_nulls)

    # Drop rows with invalid OHLC relationships
    invalid_mask = (
        (df["high"] < df["low"])
        | (df["high"] < df["open"])
        | (df["high"] < df["close"])
        | (df["low"] > df["open"])
        | (df["low"] > df["close"])
        | (df["open"] <= 0)
        | (df["close"] <= 0)
        | (df["high"] <= 0)
        | (df["low"] <= 0)
    )
    dropped_ohlc = int(invalid_mask.sum())
    if dropped_ohlc:
        log.warning("Dropped %d candles with invalid OHLC relationships", dropped_ohlc)
        df = df[~invalid_mask].copy()

    log.info("Validation: %d in → %d out (%d dropped)",
             rows_in, len(df), rows_in - len(df))
    return df
