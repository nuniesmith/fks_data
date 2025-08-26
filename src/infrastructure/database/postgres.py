"""Lightweight Postgres connection utility.

Intentionally minimal until a fuller infra layer (pooling, migrations) is
implemented. Uses psycopg (v2) if available; otherwise raises at runtime when
invoked. This mirrors the expectation in `store.py` without introducing a hard
runtime dependency for test environments that don't touch the DB.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


def _build_dsn() -> str:
    url = os.getenv("FKS_DB_URL") or os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("FKS_DB_HOST", "localhost")
    db = os.getenv("FKS_DB_NAME", "fks")
    user = os.getenv("FKS_DB_USER", "fks")
    pwd = os.getenv("FKS_DB_PASSWORD", "fks")
    port = os.getenv("FKS_DB_PORT", "5432")
    return f"dbname={db} user={user} password={pwd} host={host} port={port}"


@contextmanager
def get_connection() -> Iterator["psycopg2.extensions.connection"]:  # type: ignore
    if psycopg2 is None:  # pragma: no cover
        raise RuntimeError("psycopg2 not installed in current environment")
    dsn = _build_dsn()
    conn = psycopg2.connect(dsn)  # type: ignore
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:  # pragma: no cover
            pass

__all__ = ["get_connection"]
