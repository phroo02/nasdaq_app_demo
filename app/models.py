from sqlalchemy import (
    String,
    Float,
    Integer,
    Date,
    DateTime,
    CheckConstraint,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime


class Base(DeclarativeBase):
    pass


class RawPrice(Base):
    __tablename__ = "raw_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ticker: Mapped[str] = mapped_column(String(16), nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)  # keep as text for now

    open: Mapped[float] = mapped_column(Float, nullable=True)
    high: Mapped[float] = mapped_column(Float, nullable=True)
    low: Mapped[float] = mapped_column(Float, nullable=True)
    close: Mapped[float] = mapped_column(Float, nullable=True)

    source_file: Mapped[str] = mapped_column(String(255), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_raw_prices_ticker_date", "ticker", "date"),
    )


class Price(Base):
    __tablename__ = "prices"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    date: Mapped[str] = mapped_column(String(10), primary_key=True)  # YYYY-MM-DD

    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        CheckConstraint("high >= low", name="ck_prices_high_ge_low"),
        CheckConstraint("open > 0", name="ck_prices_open_gt_0"),
        CheckConstraint("close > 0", name="ck_prices_close_gt_0"),
        Index("ix_prices_date", "date"),
    )


class FeatureDaily(Base):
    __tablename__ = "features_daily"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    date: Mapped[str] = mapped_column(String(10), primary_key=True)

    return_1d: Mapped[float] = mapped_column(Float, nullable=True)
    range_pct: Mapped[float] = mapped_column(Float, nullable=True)
    ma_5: Mapped[float] = mapped_column(Float, nullable=True)
    ma_20: Mapped[float] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("ix_features_date", "date"),
    )
class IngestionFile(Base):
    __tablename__ = "ingestion_files"

    file_path: Mapped[str] = mapped_column(String(500), primary_key=True)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    mtime: Mapped[float] = mapped_column(Float, nullable=False)  # modification time (timestamp)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
