from datetime import datetime

from sqlalchemy import (
    BigInteger, DateTime, Float, Index, Integer, String,
    UniqueConstraint, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class InstrumentCandle(Base):

    __tablename__ = "instrument_candles"
    __table_args__ = (
        UniqueConstraint("time", "instrument", "granularity",
                         name="uq_instrument_candle"),
        Index("ix_candles_instrument_granularity_time",
              "instrument", "granularity", "time"),
        Index("ix_candles_time", "time"),
    )

    time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, primary_key=True)
    instrument: Mapped[str] = mapped_column(
        String(20), nullable=False, primary_key=True)
    granularity: Mapped[str] = mapped_column(
        String(5), nullable=False, primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return (
            f"InstrumentCandle(instrument={self.instrument!r}, "
            f"granularity={self.granularity!r}, "
            f"time={self.time}, close={self.close})"
        )


class InstrumentEtlLog(Base):
    """Audit log and resume cursor for the ETL pipeline.

    One row per successful run per (instrument, granularity) combination.
    Failed runs are not written here — Windmill's run history is the failure trail.
    to_date is the resume cursor read by the next scheduled run.
    """

    __tablename__ = "instrument_etl_log"
    __table_args__ = (
        Index("ix_etl_log_instrument_granularity",
              "instrument", "granularity"),
    )

    run_id: Mapped[str] = mapped_column(
        String(100), nullable=False, primary_key=True)
    instrument: Mapped[str] = mapped_column(
        String(20), nullable=False, primary_key=True)
    granularity: Mapped[str] = mapped_column(
        String(5), nullable=False, primary_key=True)
    from_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True)
    to_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True)
    rows_extracted: Mapped[int] = mapped_column(Integer, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, default=0)
    rows_loaded: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return (
            f"InstrumentEtlLog(run_id={self.run_id!r}, "
            f"instrument={self.instrument!r}, "
            f"rows_loaded={self.rows_loaded})"
        )
