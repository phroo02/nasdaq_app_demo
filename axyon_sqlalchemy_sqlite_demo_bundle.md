# Axyon SQLite + SQLAlchemy Demo (One-file bundle)

This file contains **all** project files you need. Copy each section into the indicated path.

---

## Project structure

```
nasdaq_app_demo/
  kaggleData/                  # your Kaggle CSV folder
  app/
    __init__.py
    __main__.py
    cli.py
    config.py
    db.py
    models.py
    schema.py
    ingest.py
    transform.py
    queries.py
    features.py
    feature_queries.py
    utils.py
  tests/
    test_smoke.py
  requirements.txt
  README.md
```

---

## requirements.txt

```txt
sqlalchemy>=2.0
pytest>=7.0
```

Install:

```bash
pip install -r requirements.txt
```

---

## app/__init__.py

```python
# empty
```

---

## app/__main__.py

```python
from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
```

---

## app/config.py

```python
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Allow overriding DB path for tests / CI
_db_path_env = os.getenv("DB_PATH")
DB_PATH = Path(_db_path_env) if _db_path_env else (PROJECT_ROOT / "data_platform.db")

DATABASE_URL = f"sqlite:///{DB_PATH}"
```

---

## app/db.py

```python
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .config import DATABASE_URL


def get_engine(echo: bool = False) -> Engine:
    return create_engine(DATABASE_URL, echo=echo)


def get_session(echo: bool = False) -> Session:
    engine = get_engine(echo=echo)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return SessionLocal()


def test_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
```

---

## app/models.py

```python
from datetime import datetime

from sqlalchemy import (
    String,
    Float,
    Integer,
    DateTime,
    CheckConstraint,
    Index,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawPrice(Base):
    __tablename__ = "raw_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ticker: Mapped[str] = mapped_column(String(16), nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)  # YYYY-MM-DD

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
    date: Mapped[str] = mapped_column(String(10), primary_key=True)

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
    mtime: Mapped[float] = mapped_column(Float, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
```

---

## app/schema.py

```python
from .db import get_engine
from .models import Base


def init_db(echo: bool = False) -> None:
    engine = get_engine(echo=echo)
    Base.metadata.create_all(engine)
```

---

## app/ingest.py

```python
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from sqlalchemy import insert, select, delete
from sqlalchemy.engine import Engine

from .db import get_engine
from .models import RawPrice, IngestionFile


def _iter_csv_files(folder: Path) -> list[Path]:
    return sorted(folder.rglob("*.csv"))


def _file_already_loaded(conn, file_path: str, file_size: int, mtime: float) -> bool:
    q = select(IngestionFile.file_path).where(
        IngestionFile.file_path == file_path,
        IngestionFile.file_size == file_size,
        IngestionFile.mtime == mtime,
    )
    return conn.execute(q).first() is not None


def _mark_file_loaded(conn, file_path: str, file_size: int, mtime: float) -> None:
    conn.execute(delete(IngestionFile).where(IngestionFile.file_path == file_path))
    conn.execute(
        insert(IngestionFile).values(
            file_path=file_path,
            file_size=file_size,
            mtime=mtime,
        )
    )


def _parse_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ingest_folder(folder: str, force: bool = False, max_files: Optional[int] = None, chunk_size: int = 5000) -> None:
    engine: Engine = get_engine(echo=False)
    folder_path = Path(folder).resolve()
    files = _iter_csv_files(folder_path)

    if max_files is not None:
        files = files[:max_files]

    total_inserted = 0
    processed_files = 0
    skipped_files = 0

    with engine.begin() as conn:
        for f in files:
            stat = f.stat()
            file_path_str = str(f)

            if (not force) and _file_already_loaded(conn, file_path_str, stat.st_size, stat.st_mtime):
                skipped_files += 1
                continue

            rows: list[dict] = []
            inserted_this_file = 0

            with f.open("r", newline="") as fp:
                reader = csv.DictReader(fp)
                for r in reader:
                    ticker = (r.get("ticker") or "").strip()
                    date = (r.get("date") or "").strip()

                    if not ticker or len(date) != 10:
                        continue

                    rows.append(
                        {
                            "ticker": ticker,
                            "date": date,
                            "open": _parse_float(r.get("open")),
                            "high": _parse_float(r.get("high")),
                            "low": _parse_float(r.get("low")),
                            "close": _parse_float(r.get("close")),
                            "source_file": file_path_str,
                        }
                    )

                    if len(rows) >= chunk_size:
                        conn.execute(insert(RawPrice), rows)
                        inserted_this_file += len(rows)
                        rows.clear()

            if rows:
                conn.execute(insert(RawPrice), rows)
                inserted_this_file += len(rows)
                rows.clear()

            _mark_file_loaded(conn, file_path_str, stat.st_size, stat.st_mtime)

            processed_files += 1
            total_inserted += inserted_this_file

    print(f"Files processed: {processed_files}")
    print(f"Files skipped:   {skipped_files}")
    print(f"Rows inserted:   {total_inserted}")
```

---

## app/transform.py

```python
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import get_engine


def build_curated(reset: bool = True) -> None:
    engine: Engine = get_engine(echo=False)

    insert_sql = text("""
    INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close)
    SELECT ticker, date, open, high, low, close
    FROM (
        SELECT
            ticker, date, open, high, low, close,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, date
                ORDER BY ingested_at DESC, id DESC
            ) AS rn
        FROM raw_prices
        WHERE
            open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
            AND high >= low
            AND open > 0
            AND close > 0
    )
    WHERE rn = 1;
    """)

    with engine.begin() as conn:
        if reset:
            conn.execute(text("DELETE FROM prices;"))
        conn.execute(insert_sql)

        raw_cnt = conn.execute(text("SELECT COUNT(*) FROM raw_prices;")).scalar_one()
        curated_cnt = conn.execute(text("SELECT COUNT(*) FROM prices;")).scalar_one()
        distinct_pairs = conn.execute(
            text("SELECT COUNT(*) FROM (SELECT DISTINCT ticker, date FROM raw_prices);")
        ).scalar_one()

    print(f"raw_prices rows:        {raw_cnt}")
    print(f"raw distinct (t,d):     {distinct_pairs}")
    print(f"curated prices rows:    {curated_cnt}")
```

---

## app/queries.py

```python
from __future__ import annotations

from typing import Optional
from sqlalchemy import select

from .db import get_session
from .models import Price


def get_prices(
    ticker: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
):
    stmt = select(Price).where(Price.ticker == ticker)

    if date_from:
        stmt = stmt.where(Price.date >= date_from)
    if date_to:
        stmt = stmt.where(Price.date <= date_to)

    stmt = stmt.order_by(Price.date.asc()).limit(limit)

    session = get_session(echo=False)
    try:
        return session.scalars(stmt).all()
    finally:
        session.close()
```

---

## app/features.py

```python
from __future__ import annotations

from typing import Optional, List

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .db import get_session
from .models import Price, FeatureDaily


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / float(len(values))


def build_features_for_ticker(ticker: str) -> int:
    session = get_session(echo=False)
    try:
        prices = session.scalars(
            select(Price).where(Price.ticker == ticker).order_by(Price.date.asc())
        ).all()

        if not prices:
            return 0

        close_history: List[float] = []
        prev_close: Optional[float] = None

        to_upsert: List[dict] = []

        for p in prices:
            close = float(p.close)
            close_history.append(close)

            ret_1d = None
            if prev_close is not None and prev_close != 0:
                ret_1d = close / prev_close - 1.0

            range_pct = None
            if close != 0:
                range_pct = (float(p.high) - float(p.low)) / close

            ma_5 = _mean(close_history[-5:]) if len(close_history) >= 5 else None
            ma_20 = _mean(close_history[-20:]) if len(close_history) >= 20 else None

            to_upsert.append(
                {
                    "ticker": p.ticker,
                    "date": p.date,
                    "return_1d": ret_1d,
                    "range_pct": range_pct,
                    "ma_5": ma_5,
                    "ma_20": ma_20,
                }
            )

            prev_close = close

        stmt = sqlite_insert(FeatureDaily).values(to_upsert)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker", "date"],
            set_={
                "return_1d": stmt.excluded.return_1d,
                "range_pct": stmt.excluded.range_pct,
                "ma_5": stmt.excluded.ma_5,
                "ma_20": stmt.excluded.ma_20,
            },
        )

        session.execute(stmt)
        session.commit()
        return len(to_upsert)
    finally:
        session.close()
```

---

## app/feature_queries.py

```python
from __future__ import annotations

from typing import Optional
from sqlalchemy import select

from .db import get_session
from .models import FeatureDaily


def get_features(
    ticker: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
):
    stmt = select(FeatureDaily).where(FeatureDaily.ticker == ticker)

    if date_from:
        stmt = stmt.where(FeatureDaily.date >= date_from)
    if date_to:
        stmt = stmt.where(FeatureDaily.date <= date_to)

    stmt = stmt.order_by(FeatureDaily.date.asc()).limit(limit)

    session = get_session(echo=False)
    try:
        return session.scalars(stmt).all()
    finally:
        session.close()
```

---

## app/utils.py

```python
from datetime import datetime
from pathlib import Path
import shutil

from .config import DB_PATH, PROJECT_ROOT


def backup_db() -> Path:
    backups_dir = PROJECT_ROOT / "backups"
    backups_dir.mkdir(exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = backups_dir / f"data_platform_{ts}.db"

    shutil.copy2(DB_PATH, backup_path)
    return backup_path
```

---

## app/cli.py

```python
import argparse

from .db import get_engine, test_connection
from .config import DB_PATH, DATABASE_URL
from .schema import init_db
from .ingest import ingest_folder
from .transform import build_curated
from .queries import get_prices
from .features import build_features_for_ticker
from .feature_queries import get_features
from .utils import backup_db


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="app", description="Axyon data platform demo")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("info", help="Show DB path and URL")
    sub.add_parser("ping-db", help="Test SQLAlchemy connection to SQLite")
    sub.add_parser("init-db", help="Create tables in the SQLite database")
    sub.add_parser("build-curated", help="Build curated prices table from raw_prices")

    load = sub.add_parser("load-raw", help="Load CSV files into raw_prices (idempotent)")
    load.add_argument("--path", required=True, help="Folder containing CSV files (e.g., kaggleData)")
    load.add_argument("--force", action="store_true", help="Reload files even if already ingested")
    load.add_argument("--max-files", type=int, default=None, help="Limit number of CSV files (for quick testing)")

    q = sub.add_parser("query-prices", help="Query prices by ticker and optional date range")
    q.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AACB)")
    q.add_argument("--from", dest="date_from", default=None, help="Start date YYYY-MM-DD")
    q.add_argument("--to", dest="date_to", default=None, help="End date YYYY-MM-DD")
    q.add_argument("--limit", type=int, default=20, help="Max rows to print")

    bf = sub.add_parser("build-features", help="Build features_daily from curated prices (one ticker)")
    bf.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AACB)")

    fq = sub.add_parser("query-features", help="Query features_daily by ticker and optional date range")
    fq.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AACB)")
    fq.add_argument("--from", dest="date_from", default=None, help="Start date YYYY-MM-DD")
    fq.add_argument("--to", dest="date_to", default=None, help="End date YYYY-MM-DD")
    fq.add_argument("--limit", type=int, default=20, help="Max rows to print")

    sub.add_parser("backup-db", help="Create a timestamped copy of the SQLite DB in ./backups")

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "info":
        print(f"DB_PATH: {DB_PATH}")
        print(f"DATABASE_URL: {DATABASE_URL}")
        return 0

    if args.command == "ping-db":
        engine = get_engine(echo=False)
        test_connection(engine)
        print("OK: DB connection works")
        return 0

    if args.command == "init-db":
        init_db(echo=False)
        print("OK: tables created (if not already present)")
        return 0

    if args.command == "load-raw":
        ingest_folder(folder=args.path, force=args.force, max_files=args.max_files)
        return 0

    if args.command == "build-curated":
        build_curated(reset=True)
        print("OK: curated prices built")
        return 0

    if args.command == "query-prices":
        rows = get_prices(
            ticker=args.ticker,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
        )
        print("ticker,date,open,high,low,close")
        for r in rows:
            print(f"{r.ticker},{r.date},{r.open},{r.high},{r.low},{r.close}")
        print(f"rows_returned: {len(rows)}")
        return 0

    if args.command == "build-features":
        n = build_features_for_ticker(args.ticker)
        print(f"OK: features built for {args.ticker}, rows_upserted={n}")
        return 0

    if args.command == "query-features":
        rows = get_features(
            ticker=args.ticker,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
        )
        print("ticker,date,return_1d,range_pct,ma_5,ma_20")
        for r in rows:
            print(f"{r.ticker},{r.date},{r.return_1d},{r.range_pct},{r.ma_5},{r.ma_20}")
        print(f"rows_returned: {len(rows)}")
        return 0

    if args.command == "backup-db":
        path = backup_db()
        print(f"OK: backup created at {path}")
        return 0

    return 1
```

---

## tests/test_smoke.py

```python
from pathlib import Path

from app.schema import init_db
from app.ingest import ingest_folder
from app.transform import build_curated
from app.features import build_features_for_ticker


def test_pipeline_smoke(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    init_db(echo=False)
    assert db_path.exists()

    ingest_folder(folder="kaggleData", max_files=1, force=True)
    build_curated(reset=True)

    n = build_features_for_ticker("AACB")
    assert n >= 0
```

Run tests:

```bash
pytest -q
```

---

## README.md (template)

```md
# Axyon Data Platform Demo (SQLite + SQLAlchemy)

## Setup
```bash
pip install -r requirements.txt
```

## Commands
```bash
python -m app init-db
python -m app load-raw --path kaggleData
python -m app build-curated
python -m app query-prices --ticker AACB --limit 5
python -m app build-features --ticker AACB
python -m app query-features --ticker AACB --limit 5
python -m app backup-db
```

## Data Model
- raw_prices: staging ingestion
- prices: curated (PK ticker+date, constraints)
- features_daily: feature engineering output
- ingestion_files: idempotent ingestion tracking
- backups/: sqlite file copies (simple DR)
```

---

## Demo script (for the interview)

```bash
python -m app init-db
python -m app load-raw --path kaggleData --max-files 3
python -m app build-curated
python -m app query-prices --ticker AACB --limit 5
python -m app build-features --ticker AACB
python -m app query-features --ticker AACB --limit 5
python -m app backup-db
```

---

## Talking points (assessment)

- SQL + Python interaction: SQLAlchemy Session for ORM queries; transactions with `engine.begin()`; chunked inserts.
- Data modeling: composite PK `(ticker, date)`; CHECK constraints; indexes.
- Data manipulation: curated build with window function dedupe; feature generation as “dataset generation tool”.
- Reliability: idempotent ingestion (`ingestion_files`) and DB backups (`backup-db`).
