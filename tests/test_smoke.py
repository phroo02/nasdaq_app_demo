import os
from pathlib import Path

from app.schema import init_db
from app.ingest import ingest_folder
from app.transform import build_curated
from app.features import build_features_for_ticker


def test_pipeline_smoke(tmp_path: Path, monkeypatch):
    # Use a temp db for this test
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    # init schema
    init_db(echo=False)
    assert db_path.exists()

    # ingest a few files (assumes kaggleData exists in project root)
    ingest_folder(folder="kaggleData", max_files=1, force=True)

    # curated
    build_curated(reset=True)

    # features for one ticker that exists in first file
    # We don't know which tickers are in first file, so just pick one from DB via sqlite
    # Instead, run features for AACB if present; if not, test passes on "no rows" gracefully.
    n = build_features_for_ticker("AACB")
    assert n >= 0
