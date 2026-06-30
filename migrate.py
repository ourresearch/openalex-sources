"""Minimal forward-only migration runner for openalex-sources.

Applies every migrations/NNN_*.sql not yet recorded in `schema_migrations`, in
filename order, each in its own transaction. Matches the team's raw-SQL migration
convention (no Alembic). Run on Heroku release and locally:

    python migrate.py
"""
import glob
import os

from sqlalchemy import text

from db import engine

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")


def main() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())"
            )
        )
        applied = {r[0] for r in conn.execute(text("SELECT version FROM schema_migrations"))}

    files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql")))
    for path in files:
        version = os.path.basename(path).split("_", 1)[0]
        if version in applied:
            print(f"skip {version} (already applied)")
            continue
        sql = open(path).read()
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)
            conn.execute(
                text("INSERT INTO schema_migrations (version) VALUES (:v)"),
                {"v": version},
            )
        print(f"applied {version} ({os.path.basename(path)})")


if __name__ == "__main__":
    main()
