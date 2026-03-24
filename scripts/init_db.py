"""Initialize the local SQLite database using the SQL schema."""

from __future__ import annotations

import sqlite3
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "app.db"
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as connection:
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        connection.executescript(schema_sql)
        connection.commit()

    print(f"Initialized database at: {DB_PATH}")


if __name__ == "__main__":
    init_db()
