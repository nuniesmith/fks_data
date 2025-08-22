"""
Active Assets storage and backfill utilities.

Persist a list of assets to track with their backfill policy and incremental
progress, and provide helpers to fetch in small chunks to avoid rate limits.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Optional imports for validation and DB persistence
try:
    from .validation import validate_ohlcv, compute_time_splits
except Exception:  # pragma: no cover - optional
    validate_ohlcv = None  # type: ignore
    compute_time_splits = None  # type: ignore
try:
    from .store import ensure_schema as _ensure_db_schema, upsert_ohlcv as _upsert_ohlcv, materialize_splits as _materialize_splits
except Exception:  # pragma: no cover - optional
    _ensure_db_schema = None  # type: ignore
    _upsert_ohlcv = None  # type: ignore
    _materialize_splits = None  # type: ignore
try:
    from .splitting import split_managed_csv as _split_managed
except Exception:  # pragma: no cover - optional
    _split_managed = None  # type: ignore

DB_PATH = os.getenv("ACTIVE_ASSETS_DB", os.path.join("data", "active_assets.db"))
DATA_DIR = os.getenv("ACTIVE_ASSETS_DIR", os.path.join("data", "managed"))
DEFAULT_RATE_DELAY_SEC = float(os.getenv("ACTIVE_ASSETS_RATE_DELAY", "2.0"))


def _utcnow() -> str:
    return datetime.utcnow().isoformat()


@dataclass
class ActiveAsset:
    id: Optional[int]
    source: str
    symbol: str
    intervals: List[str]
    asset_type: Optional[str] = None
    exchange: Optional[str] = None
    years: Optional[int] = None
    full_history: bool = False
    enabled: bool = True
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_row(self) -> Tuple:
        return (
            self.source,
            self.symbol,
            self.asset_type,
            self.exchange,
            ",".join(self.intervals),
            self.years if self.years is not None else None,
            1 if self.full_history else 0,
            1 if self.enabled else 0,
            self.created_at or _utcnow(),
            self.updated_at or _utcnow(),
        )


class ActiveAssetStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        """Return a new sqlite3 connection.

        NOTE: Callers should prefer using it via a context manager:
            with store.connection() as conn:
                ...
        to ensure proper closing and avoid ResourceWarnings in tests.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # Lightweight context manager wrapper to reduce duplicated with blocks
    def connection(self):  # pragma: no cover - trivial wrapper
        class _Ctx:
            def __init__(self, outer):
                self.outer = outer
                self.conn = None
            def __enter__(self):
                self.conn = self.outer._conn()
                return self.conn
            def __exit__(self, exc_type, exc, tb):
                try:
                    if self.conn is not None:
                        self.conn.close()
                finally:
                    self.conn = None
        return _Ctx(self)

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    asset_type TEXT NULL,
                    exchange TEXT NULL,
                    intervals TEXT NOT NULL,
                    years INTEGER NULL,
                    full_history INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            # Ensure new columns exist in legacy DBs
            def _ensure_column(table: str, col: str, type_decl: str) -> None:
                try:
                    cur = conn.execute(f"PRAGMA table_info({table})")
                    cols = [r[1] for r in cur.fetchall()]
                    if col not in cols:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_decl}")
                except Exception:
                    pass

            _ensure_column("active_assets", "asset_type", "TEXT NULL")
            _ensure_column("active_assets", "exchange", "TEXT NULL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_progress (
                    asset_id INTEGER NOT NULL,
                    interval TEXT NOT NULL,
                    last_cursor TEXT NULL,
                    target_start TEXT NULL,
                    target_end TEXT NULL,
                    last_rows INTEGER NOT NULL DEFAULT 0,
                    last_run TEXT NULL,
                    PRIMARY KEY (asset_id, interval),
                    FOREIGN KEY (asset_id) REFERENCES active_assets(id) ON DELETE CASCADE
                )
                """
            )

    def list_assets(self) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM active_assets ORDER BY id DESC")
            rows = [dict(r) for r in cur.fetchall()]
            # Attach progress per interval
            for r in rows:
                r["intervals"] = r.get("intervals", "").split(",") if r.get("intervals") else []
                prog = conn.execute(
                    "SELECT interval, last_cursor, target_start, target_end, last_rows, last_run FROM backfill_progress WHERE asset_id=?",
                    (r["id"],),
                ).fetchall()
                r["progress"] = [dict(p) for p in prog]
        return rows

    def get_asset(self, asset_id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM active_assets WHERE id=?", (asset_id,)).fetchone()
            if not row:
                return None
            out = dict(row)
            out["intervals"] = out.get("intervals", "").split(",") if out.get("intervals") else []
            prog = conn.execute(
                "SELECT interval, last_cursor, target_start, target_end, last_rows, last_run FROM backfill_progress WHERE asset_id=?",
                (asset_id,),
            ).fetchall()
            out["progress"] = [dict(p) for p in prog]
            return out

    def add_asset(self, asset: ActiveAsset) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO active_assets (source, symbol, asset_type, exchange, intervals, years, full_history, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                asset.to_row(),
            )
            asset_id = int(cur.lastrowid or 0)

            # Initialize progress per interval
            now = datetime.utcnow()
            target_end = now
            if asset.full_history:
                # Start sufficiently far in the past; provider will clamp to earliest available
                target_start = now - timedelta(days=365 * 20)
            else:
                years = asset.years or 1
                target_start = now - timedelta(days=365 * years)

            for itv in asset.intervals:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO backfill_progress (asset_id, interval, last_cursor, target_start, target_end, last_rows, last_run)
                    VALUES (?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        asset_id,
                        itv,
                        target_start.isoformat(),
                        target_start.isoformat() if target_start else None,
                        target_end.isoformat(),
                        0,
                    ),
                )
        return asset_id

    def remove_asset(self, asset_id: int) -> bool:
        with self._conn() as conn:
            conn.execute("DELETE FROM backfill_progress WHERE asset_id=?", (asset_id,))
            cur = conn.execute("DELETE FROM active_assets WHERE id=?", (asset_id,))
            return cur.rowcount > 0

    def set_enabled(self, asset_id: int, enabled: bool) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE active_assets SET enabled=?, updated_at=? WHERE id=?",
                (1 if enabled else 0, _utcnow(), asset_id),
            )

    def advance_cursor(self, asset_id: int, interval: str, new_cursor: datetime, rows: int) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE backfill_progress
                SET last_cursor=?, last_rows=?, last_run=?
                WHERE asset_id=? AND interval=?
                """,
                (new_cursor.isoformat(), rows, _utcnow(), asset_id, interval),
            )

    def get_progress(self, asset_id: int, interval: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM backfill_progress WHERE asset_id=? AND interval=?",
                (asset_id, interval),
            ).fetchone()
            return dict(row) if row else None


def ensure_data_dir(path: str = DATA_DIR) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def csv_path_for(source: str, symbol: str, interval: str) -> Path:
    safe_symbol = symbol.replace("/", "-").replace(":", "-")
    p = Path(DATA_DIR) / source / safe_symbol
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{safe_symbol}_{interval}.csv"


def append_csv(df: pd.DataFrame, out_path: Path) -> int:
    # Normalize columns
    df = df.copy()
    # Identify datetime column
    dt_col: Optional[str] = None
    for c in df.columns:
        if "date" in c.lower() or "time" in c.lower():
            dt_col = c
            break
    if dt_col is None:
        return 0
    df[dt_col] = pd.to_datetime(df[dt_col])
    df = df.sort_values(dt_col)

    # If file exists, read and concatenate with dedupe by datetime
    if out_path.exists():
        try:
            existing = pd.read_csv(out_path)
            for c in existing.columns:
                if "date" in c.lower() or "time" in c.lower():
                    existing[c] = pd.to_datetime(existing[c])
                    dt_existing = c
                    break
            else:
                dt_existing = None
            if dt_existing:
                all_df = pd.concat([existing, df], ignore_index=True)
                all_df[dt_existing] = pd.to_datetime(all_df[dt_existing])
                all_df = all_df.drop_duplicates(subset=[dt_existing]).sort_values(dt_existing)
                all_df.to_csv(out_path, index=False)
                return len(df)
        except Exception:
            # Fall back to overwrite
            df.to_csv(out_path, index=False)
            return len(df)

    # Fresh write
    df.to_csv(out_path, index=False)
    return len(df)


class BackfillScheduler:
    """Very lightweight background scheduler that advances each asset by one chunk per cycle."""

    def __init__(self, store: ActiveAssetStore, rate_delay_sec: float = DEFAULT_RATE_DELAY_SEC):
        self.store = store
        self.rate_delay_sec = rate_delay_sec
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self, fetcher_cb) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _run_loop():
            ensure_data_dir()
            # Ensure DB schema exists if available
            try:
                if _ensure_db_schema:
                    _ensure_db_schema()
            except Exception:
                pass
            while not self._stop.is_set():
                try:
                    assets = self.store.list_assets()
                    for a in assets:
                        if not a.get("enabled"):
                            continue
                        for itv in a.get("intervals", []):
                            prog = self.store.get_progress(a["id"], itv)
                            if not prog:
                                continue
                            # Determine chunk size by interval
                            chunk_days = 1
                            if itv in ("1d", "1w", "1M"):
                                chunk_days = 30
                            elif itv in ("1h", "4h"):
                                chunk_days = 7
                            else:
                                chunk_days = 2

                            start_dt = datetime.fromisoformat(prog["last_cursor"]) if prog.get("last_cursor") else None
                            end_dt = start_dt + timedelta(days=chunk_days) if start_dt else datetime.utcnow()
                            target_end = datetime.fromisoformat(prog["target_end"]) if prog.get("target_end") else datetime.utcnow()

                            # Stop if we've passed target_end
                            if start_dt and start_dt >= target_end:
                                continue

                            df = fetcher_cb(
                                source=a["source"],
                                symbol=a["symbol"],
                                interval=itv,
                                start_date=start_dt,
                                end_date=min(end_dt, target_end),
                            )
                            rows = int(len(df)) if df is not None else 0
                            if rows > 0:
                                # Validate
                                if validate_ohlcv is not None:
                                    try:
                                        rep = validate_ohlcv(df)
                                        # Skip chunk if too poor quality
                                        if rep.missing_pct > 50.0:
                                            rows = 0
                                    except Exception:
                                        pass
                                if rows > 0:
                                    out = csv_path_for(a["source"], a["symbol"], itv)
                                    append_csv(df, out)
                                    # Upsert to TimescaleDB/Postgres if available
                                    try:
                                        if _upsert_ohlcv:
                                            _upsert_ohlcv(a["source"], a["symbol"], itv, df)
                                    except Exception:
                                        pass
                            # Advance cursor even if empty to avoid stalls
                            self.store.advance_cursor(a["id"], itv, end_dt, rows)

                            # If we've finished the target range, optionally compute dataset splits
                            try:
                                if start_dt and end_dt >= target_end and _split_managed and compute_time_splits and _materialize_splits:
                                    # Write split CSVs and persist split boundaries in DB
                                    _split_managed(a["source"], a["symbol"], itv)
                                    # Load full managed CSV to compute boundaries for DB
                                    out_csv = csv_path_for(a["source"], a["symbol"], itv)
                                    if out_csv.exists():
                                        df_all = pd.read_csv(out_csv)
                                        splits = compute_time_splits(df_all)
                                        _materialize_splits(a["source"], a["symbol"], itv, splits)
                            except Exception:
                                pass

                            # Rate limit spacing
                            time.sleep(self.rate_delay_sec)
                except Exception:
                    # don't crash the loop
                    time.sleep(self.rate_delay_sec)
                # Sleep between cycles
                time.sleep(max(15.0, self.rate_delay_sec))

        self._stop.clear()
        self._thread = threading.Thread(target=_run_loop, name="ActiveAssetsBackfill", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
