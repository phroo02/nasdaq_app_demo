from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from sqlalchemy import insert, select, delete
from sqlalchemy.engine import Engine

from .db import get_engine
from .models import RawPrice, IngestionFile


def _iter_csv_files(folder: Path) -> list[Path]:
    # Reads all .csv files under the folder (recursive)
    return sorted(folder.rglob("*.csv"))


def _file_already_loaded(conn, file_path: str, file_size: int, mtime: float) -> bool:
    q = select(IngestionFile.file_path).where(
        IngestionFile.file_path == file_path,
        IngestionFile.file_size == file_size,
        IngestionFile.mtime == mtime,
    )
    return conn.execute(q).first() is not None


def _mark_file_loaded(conn, file_path: str, file_size: int, mtime: float) -> None:
    # Simple approach: delete any existing record for file_path, then insert the current metadata
    conn.execute(delete(IngestionFile).where(IngestionFile.file_path == file_path))
    conn.execute(
        insert(IngestionFile).values(
            file_path=file_path,
            file_size=file_size,
            mtime=mtime,
        )
    )


def _parse_float(s: str) -> float | None:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ingest_folder(folder: str, force: bool = False, max_files: int | None = None, chunk_size: int = 5000) -> None:
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
                        # skip malformed rows
                        continue

                    rows.append(
                        {
                            "ticker": ticker,
                            "date": date,
                            "open": _parse_float(r.get("open", "")),
                            "high": _parse_float(r.get("high", "")),
                            "low": _parse_float(r.get("low", "")),
                            "close": _parse_float(r.get("close", "")),
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
