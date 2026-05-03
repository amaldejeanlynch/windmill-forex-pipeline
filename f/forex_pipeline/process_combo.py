"""Windmill fan-out worker script — processes one (instrument, granularity) combo.

Receives inputs from the flow's for-each iterator plus shared context from the
prelude step. Windmill injects the OANDA resource dict directly as `oanda`.

Pipeline: extract → validate → load → log
Any exception propagates to Windmill, which applies the retry policy and marks
the combo as failed. Other combos run unaffected (skip_failures=true on the flow).
"""
import logging
from datetime import datetime

import wmill

from f.forex_pipeline.lib.database import DatabaseManager
from f.forex_pipeline.lib.oanda_client import OandaClient
from f.forex_pipeline.lib.validator import validate_candles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(__name__)


def main(
    oanda: dict,         # c_oanda resource — injected by Windmill
    instrument: str,
    granularity: str,
    from_dt: str,        # ISO8601 string from the prelude work item
    to_dt: str,          # ISO8601 string from the prelude output
    run_id: str,         # shared UUID for this flow run
    started_at: str,     # ISO8601 string — when the flow started
) -> dict:
    database_url = wmill.get_variable("f/forex_pipeline/database_url")

    from_dt_d = datetime.fromisoformat(from_dt.replace("Z", "+00:00"))
    to_dt_d = datetime.fromisoformat(to_dt.replace("Z", "+00:00"))
    started_at_d = datetime.fromisoformat(started_at.replace("Z", "+00:00"))

    log.info("Processing %s %s | %s → %s", instrument, granularity, from_dt_d, to_dt_d)

    # Extract
    client = OandaClient(
        api_token=oanda["api_token"],
        account_id=oanda["account_id"],
        base_url=oanda["base_url"],
    )
    df = client.get_candles_range(instrument, granularity, from_dt_d, to_dt_d)
    rows_extracted = len(df)

    # Validate
    df = validate_candles(df)
    rows_valid = len(df)

    # Load
    db = DatabaseManager.from_url(database_url)
    try:
        rows_loaded = db.upsert_candles(df)

        # Use the latest candle time as the resume cursor so the next run
        # starts exactly where this one ended, not at the requested to_dt
        actual_to_date = (
            df["time"].max().to_pydatetime()
            if rows_loaded > 0 and not df.empty
            else to_dt_d
        )

        db.log_pipeline_run(
            run_id=run_id,
            instrument=instrument,
            granularity=granularity,
            from_date=from_dt_d,
            to_date=actual_to_date,
            rows_extracted=rows_extracted,
            rows_valid=rows_valid,
            rows_loaded=rows_loaded,
            started_at=started_at_d,
        )
    finally:
        db.close()

    log.info(
        "Done %s %s | extracted=%d valid=%d loaded=%d",
        instrument, granularity, rows_extracted, rows_valid, rows_loaded,
    )

    return {
        "instrument": instrument,
        "granularity": granularity,
        "rows_extracted": rows_extracted,
        "rows_valid": rows_valid,
        "rows_loaded": rows_loaded,
    }
