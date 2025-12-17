from __future__ import annotations

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
