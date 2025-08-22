"""
PostgreSQL storage facade for OHLCV data and dataset splits.

Tables (TimescaleDB-first schema, compatible with plain Postgres):
  - ohlcv (source, symbol, interval, ts, open, high, low, close, volume, PRIMARY KEY (source, symbol, interval, ts))
  - dataset_splits (source, symbol, interval, split, start_ts, end_ts, created_at)
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from loguru import logger

from infrastructure.database.postgres import get_connection


DDL = {
    "ohlcv": """
        CREATE TABLE IF NOT EXISTS ohlcv (
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            PRIMARY KEY (source, symbol, interval, ts)
        );
    """,
    "dataset_splits": """
        CREATE TABLE IF NOT EXISTS dataset_splits (
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            split TEXT NOT NULL,
            start_ts TIMESTAMPTZ NOT NULL,
            end_ts TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source, symbol, interval, split)
        );
    """,
}


def ensure_schema() -> None:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for sql in DDL.values():
                    cur.execute(sql)
            conn.commit()
    except Exception as e:
        logger.warning(f"ensure_schema error: {e}")


def upsert_ohlcv(source: str, symbol: str, interval: str, df: pd.DataFrame) -> int:
    """Upsert OHLCV rows into TimescaleDB/Postgres. Returns number of rows processed."""
    if df is None or df.empty:
        return 0
    # find datetime col
    dt_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "date" in cl or "time" in cl:
            dt_col = c
            break
    if dt_col is None:
        return 0
    work = df.copy()
    work[dt_col] = pd.to_datetime(work[dt_col])
    cols = {k: k for k in ["open", "high", "low", "close", "volume"] if k in work.columns}

    rows: List[Tuple] = []
    for _, r in work.iterrows():
        row = [source, symbol, interval, r[dt_col]]
        vals: List[Optional[float]] = []
        for k in ["open", "high", "low", "close", "volume"]:
            if k in cols and k in r and pd.notna(r[k]):
                try:
                    vals.append(float(r[k]))
                except Exception:
                    vals.append(None)
            else:
                vals.append(None)
        row += vals
        rows.append(tuple(row))

    sql = (
        "INSERT INTO ohlcv (source, symbol, interval, ts, open, high, low, close, volume) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (source, symbol, interval, ts) DO UPDATE SET "
        "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume"
    )
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)
    except Exception as e:
        logger.warning(f"upsert_ohlcv error: {e}")
        return 0


def materialize_splits(source: str, symbol: str, interval: str, splits: List[Tuple[str, pd.Timestamp, pd.Timestamp]]) -> int:
    if not splits:
        return 0
    values = [(source, symbol, interval, s, a.to_pydatetime(), b.to_pydatetime()) for s, a, b in splits]
    sql = (
        "INSERT INTO dataset_splits (source, symbol, interval, split, start_ts, end_ts) "
        "VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (source, symbol, interval, split) DO UPDATE SET start_ts=EXCLUDED.start_ts, end_ts=EXCLUDED.end_ts"
    )
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
            conn.commit()
        return len(values)
    except Exception as e:
        logger.warning(f"materialize_splits error: {e}")
        return 0
