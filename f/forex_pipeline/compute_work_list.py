"""Windmill prelude script — builds the work list for fan-out.

Scheduled run (from_date=None):
    Queries instrument_etl_log for the last successful to_date per
    (instrument, granularity) combination. Each combo starts from its own
    cursor, or PIPELINE_START_DATE for combos with no history.

Backfill run (from_date provided):
    Ignores instrument_etl_log entirely. Every combo starts from the
    explicit from_date. Pass instruments_filter to restrict scope.

Returns a dict consumed by the Windmill flow for-each fan-out:
    {
        "run_id": "uuid",
        "started_at": "ISO8601",
        "to_dt": "ISO8601",
        "work_items": [{"instrument": ..., "granularity": ..., "from_dt": ...}, ...]
    }
"""
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

import wmill

from f.forex_pipeline.lib.config import GRANULARITIES, INSTRUMENTS, PIPELINE_START_DATE
from f.forex_pipeline.lib.database import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(__name__)


def main(
    from_date: str | None = None,
    instruments_filter: list[str] | None = None,
) -> dict:
    database_url = wmill.get_variable("f/forex_pipeline/database_url")

    instruments = INSTRUMENTS
    if instruments_filter:
        instruments = [i for i in instruments if i in instruments_filter]

    started_at = datetime.now(timezone.utc)
    to_dt = started_at

    if from_date:
        # Backfill mode — ignore DB, apply explicit date to all combos
        from_dt = datetime.fromisoformat(from_date.replace("Z", "+00:00"))
        work_items = [
            {
                "instrument": inst,
                "granularity": gran,
                "from_dt": from_dt.isoformat(),
            }
            for inst in instruments
            for gran in GRANULARITIES
        ]
        run_id = f"backfill-{uuid4()}"
        log.info("Backfill mode | from_date=%s | %d work items", from_date, len(work_items))

    else:
        # Scheduled mode — query DB for last successful cursor per combo
        db = DatabaseManager.from_url(database_url)
        db.create_tables()
        last_fetch = db.get_all_last_fetch_dates()
        db.close()

        work_items = []
        for inst in instruments:
            for gran in GRANULARITIES:
                from_dt = last_fetch.get((inst, gran), PIPELINE_START_DATE)
                work_items.append({
                    "instrument": inst,
                    "granularity": gran,
                    "from_dt": from_dt.isoformat(),
                })
        run_id = str(uuid4())
        log.info("Scheduled mode | %d combos | run_id=%s", len(work_items), run_id)

    return {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "to_dt": to_dt.isoformat(),
        "work_items": work_items,
    }
