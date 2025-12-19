import sqlite3
from pathlib import Path

# Database file location
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "scheduler.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def run_migrations():
    conn = get_connection()
    try:
        with open(BASE_DIR / "storage" / "migrations.sql", "r") as f:
            sql = f.read()
        conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()
