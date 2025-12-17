import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Allow overriding DB path for tests
_db_path_env = os.getenv("DB_PATH")

DB_PATH = Path(_db_path_env) if _db_path_env else (PROJECT_ROOT / "data_platform.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"
