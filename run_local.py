"""Local runner — executes the pipeline without Windmill.

Reads credentials from .env, builds the same resource dicts that Windmill
would inject, then runs the full extract → validate → load loop locally.

Usage:
    python run_local.py

To test a single instrument/granularity:
    python run_local.py  (then modify the call at the bottom)
"""
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from dotenv import load_dotenv

from f.forex_pipeline.lib.config import GRANULARITIES, INSTRUMENTS, PIPELINE_START_DATE
from f.forex_pipeline.lib.database import DatabaseManager
from f.forex_pipeline.lib.oanda_client import OandaClient
from f.forex_pipeline.lib.validator import validate_candles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(__name__)


def run_pipeline(
    instruments: list[str] | None = None,
    granularities: list[str] | None = None,
    from_date: datetime | None = None,
) -> None:
    """Run the full pipeline locally using .env credentials.

    Args:
        instruments: Subset of instruments to run. Defaults to all.
        granularities: Subset of granularities to run. Defaults to all.
        from_date: Override start date for all combos (backfill mode).
                   If None, reads resume cursors from the DB.
    """
    load_dotenv()

    database_url = os.environ["DATABASE_URL"]
    api_token = os.environ["OANDA_API_TOKEN"]
    account_id = os.environ["OANDA_ACCOUNT_ID"]
    base_url = os.environ["OANDA_BASE_URL"]

    instruments = instruments or INSTRUMENTS
    granularities = granularities or GRANULARITIES

    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    to_dt = started_at

    log.info("Local run | run_id=%s | %d instruments × %d granularities",
             run_id, len(instruments), len(granularities))

    db = DatabaseManager.from_url(database_url)
    db.create_tables()

    last_fetch = db.get_all_last_fetch_dates() if from_date is None else {}

    for inst in instruments:
        for gran in granularities:
            combo_from = from_date or last_fetch.get((inst, gran), PIPELINE_START_DATE)

            try:
                client = OandaClient(
                    api_token=api_token,
                    account_id=account_id,
                    base_url=base_url,
                )
                df = client.get_candles_range(inst, gran, combo_from, to_dt)
                rows_extracted = len(df)

                df = validate_candles(df)
                rows_valid = len(df)

                rows_loaded = db.upsert_candles(df)

                actual_to_date = (
                    df["time"].max().to_pydatetime()
                    if rows_loaded > 0 and not df.empty
                    else to_dt
                )

                db.log_pipeline_run(
                    run_id=run_id,
                    instrument=inst,
                    granularity=gran,
                    from_date=combo_from,
                    to_date=actual_to_date,
                    rows_extracted=rows_extracted,
                    rows_valid=rows_valid,
                    rows_loaded=rows_loaded,
                    started_at=started_at,
                )

            except Exception as exc:
                log.error("Failed %s %s: %s", inst, gran, exc)

    db.close()
    log.info("Local run %s complete", run_id)


if __name__ == "__main__":
    run_pipeline()
