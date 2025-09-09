"""Simple migration runner for fks_data.

Executes SQL files in lexical order from ../migrations. Idempotent for CREATE IF NOT EXISTS.
Future: track applied migrations in a migrations table.
"""
from __future__ import annotations
import os
from pathlib import Path
from loguru import logger
import hashlib

try:
    from infrastructure.database.postgres import get_connection  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(f"Database module unavailable: {e}")

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def run() -> None:
    files = sorted(p for p in MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        logger.info("No migration files found.")
        return
    applied = set()
    with get_connection() as conn:  # type: ignore
        with conn.cursor() as cur:
            # ensure tracking table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS fks_schema_migrations (
                    filename TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute("SELECT filename, checksum FROM fks_schema_migrations")
            for row in cur.fetchall():
                applied.add(row[0])
                # verify checksum integrity
                fname, stored_checksum = row
                path = MIGRATIONS_DIR / fname
                if path.exists():
                    current = hashlib.sha256(path.read_bytes()).hexdigest()
                    if current != stored_checksum:
                        raise SystemExit(
                            f"Checksum mismatch for migration {fname}: stored={stored_checksum} current={current}. Refuse to start."
                        )
            applied_count = 0
            for sql_file in files:
                if sql_file.name in applied:
                    logger.debug(f"Skipping already applied migration {sql_file.name}")
                    continue
                sql = sql_file.read_text(encoding="utf-8")
                checksum = hashlib.sha256(sql.encode()).hexdigest()
                logger.info(f"Applying migration {sql_file.name}")
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO fks_schema_migrations (filename, checksum) VALUES (%s,%s)",
                    (sql_file.name, checksum),
                )
                applied_count += 1
        conn.commit()
    logger.success(f"Applied {applied_count} new migration(s). Total tracked: {len(applied) + applied_count}.")


if __name__ == "__main__":  # pragma: no cover
    run()
