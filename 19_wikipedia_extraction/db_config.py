"""
db_config.py
=============
Shared database configuration loaded from .env file.
Import this in all wikipedia extraction scripts instead of hardcoding credentials.

Usage:
    from db_config import get_db, DB_CONFIG
"""

import os
from pathlib import Path

# Load .env file from the same folder as this script
_env_path = Path(__file__).parent / ".env"

if _env_path.exists():
    with open(_env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())
else:
    print(f"[WARN] .env not found at {_env_path}. Using defaults.")

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "user":     os.environ.get("DB_USER",     "root"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "database": os.environ.get("DB_NAME",     "census_india_2011"),
}


def get_db():
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)
