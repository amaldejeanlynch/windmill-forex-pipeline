import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from f.forex_pipeline.lib.models import Base, InstrumentCandle, InstrumentEtlLog

log = logging.getLogger(__name__)

BATCH_SIZE: int = 1000


def _df_to_candle_records(df: pd.DataFrame) -> list[dict]:
    """Convert a validated candle DataFrame to a list of dicts for DB insert.

    Selects only the 8 core columns. Converts the timezone-aware datetime
    column to plain Python datetimes (required by psycopg2).
    """
    cols = ["time", "instrument", "granularity",
            "open", "high", "low", "close", "volume"]
    subset = df[cols].copy()
    # List comprehension calls .to_pydatetime() on individual Timestamps, avoiding
    # the FutureWarning raised by the deprecated .dt.to_pydatetime() Series accessor.
    subset["time"] = [ts.to_pydatetime() for ts in subset["time"]]
    return subset.to_dict(orient="records")


class DatabaseManager:

    def __init__(self, database_url: str) -> None:
        self._engine = create_engine(
            database_url,
            pool_size=2,
            max_overflow=1,
            pool_pre_ping=True,
            pool_timeout=30,
            connect_args={"connect_timeout": 10},
        )
        self._Session = sessionmaker(bind=self._engine)

    @classmethod
    def from_url(cls, database_url: str) -> "DatabaseManager":
        """Construct from a full SQLAlchemy connection URL string."""
        return cls(database_url=database_url)

    @contextmanager
    def _session(self) -> Generator[Session, None, None]:
        """Transactional session: commits on success, rolls back on any exception."""
        session = self._Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self) -> None:
        Base.metadata.create_all(self._engine)
        log.info("Tables created or verified")

    def close(self) -> None:
        self._engine.dispose()

    def get_all_last_fetch_dates(self) -> dict[tuple[str, str], datetime]:
        """Single bulk query: last successful to_date per (instrument, granularity).

        Uses PostgreSQL DISTINCT ON to get the latest cursor per combo in one
        round-trip — avoids 88 individual queries per scheduled run.
        Returns a dict for O(1) lookups in the work-list builder.
        """
        query = text("""
            SELECT DISTINCT ON (instrument, granularity)
                instrument, granularity, to_date
            FROM instrument_etl_log
            WHERE to_date IS NOT NULL
            ORDER BY instrument, granularity, to_date DESC
        """)
        with self._session() as session:
            result = session.execute(query)
            return {(row[0], row[1]): row[2] for row in result}

    def upsert_candles(self, df: pd.DataFrame) -> int:
        """Upsert candles in batches. Returns count of new rows inserted.

        ON CONFLICT DO NOTHING means re-running the same window is safe —
        existing candles are never overwritten (idempotent).
        """
        if df.empty:
            return 0

        records = _df_to_candle_records(df)
        rows_inserted = 0

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i: i + BATCH_SIZE]
            with self._session() as session:
                stmt = (
                    insert(InstrumentCandle)
                    .values(batch)
                    .on_conflict_do_nothing(constraint="uq_instrument_candle")
                )
                rows_inserted += session.execute(stmt).rowcount

        log.info("Upsert: %d new rows inserted (of %d attempted)",
                 rows_inserted, len(records))
        return rows_inserted

    def log_pipeline_run(
        self,
        run_id: str,
        instrument: str,
        granularity: str,
        from_date: datetime,
        to_date: datetime,
        rows_extracted: int,
        rows_valid: int,
        rows_loaded: int,
        started_at: datetime,
    ) -> None:
        """Write a success row to instrument_etl_log.

        Uses ON CONFLICT DO NOTHING so Windmill retries (which reuse the same
        run_id from the prelude step) never raise an IntegrityError on the PK.
        """
        stmt = (
            insert(InstrumentEtlLog)
            .values(
                run_id=run_id,
                instrument=instrument,
                granularity=granularity,
                from_date=from_date,
                to_date=to_date,
                rows_extracted=rows_extracted,
                rows_valid=rows_valid,
                rows_loaded=rows_loaded,
                started_at=started_at,
            )
            .on_conflict_do_nothing()
        )
        with self._session() as session:
            session.execute(stmt)
