"""Market bar utilities (Week 3 DB schema groundwork).

Provides conversion from adapter fetch output -> List[MarketBar] plus a thin
repository scaffold wrapping existing upsert helpers. Actual DB round‑trip
tests are deferred until a Postgres test fixture is available; current tests
focus on pure transformation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List
from pathlib import Path
import json
try:
    from jsonschema import Draft202012Validator  # type: ignore
except Exception:  # pragma: no cover
    Draft202012Validator = None  # type: ignore

from shared_python.types import MarketBar  # type: ignore

try:  # reuse existing upsert logic for now
    from store import upsert_ohlcv  # type: ignore
except Exception:  # pragma: no cover
    upsert_ohlcv = None  # type: ignore


_SCHEMA_VALIDATOR = None


def _get_validator():  # lazy load
    global _SCHEMA_VALIDATOR
    if _SCHEMA_VALIDATOR is not None:
        return _SCHEMA_VALIDATOR
    if Draft202012Validator is None:  # pragma: no cover
        return None
    schema_path = Path(__file__).resolve().parents[3] / "shared" / "shared_schema" / "bars" / "v1" / "market_bar.schema.json"
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        _SCHEMA_VALIDATOR = Draft202012Validator(schema)
    except Exception:  # pragma: no cover
        _SCHEMA_VALIDATOR = None
    return _SCHEMA_VALIDATOR


def to_market_bars(adapter_result: dict, *, provider_override: str | None = None, validate: bool = True):  # -> List[MarketBar]
    """Convert normalized adapter result -> List[MarketBar].

    Expects shape: { provider: str, data: [ {ts, open, high, low, close, volume, ...}, ... ] }
    """
    provider = provider_override or adapter_result.get("provider")
    rows = adapter_result.get("data") or []
    out = []
    validator = _get_validator() if validate else None
    for r in rows:
        try:
            if validator is not None:
                validator.validate(r)
            out.append(
                MarketBar(
                    ts=int(r["ts"]),
                    open=float(r["open"]),
                    high=float(r["high"]),
                    low=float(r["low"]),
                    close=float(r["close"]),
                    volume=float(r.get("volume", 0.0)),
                    provider=provider,
                )
            )
        except Exception:  # pragma: no cover - skip malformed row
            continue
    return out


class BarRepository:
    """Persistence facade for MarketBar objects.

    Current implementation delegates to existing DataFrame upsert helper to
    avoid duplicating SQL; a future migration can replace with direct COPY or
    bulk INSERT for performance.
    """

    def upsert(self, *, provider: str, symbol: str, interval: str, bars: Iterable[MarketBar]) -> int:  # type: ignore[name-defined]
        import pandas as pd  # local import to keep base deps light

        bar_list = list(bars)
        if not bar_list:
            return 0
        if upsert_ohlcv is None:  # pragma: no cover
            raise RuntimeError("upsert helper unavailable")
        df = pd.DataFrame(
            [
                {
                    "provider": b.provider or provider,
                    "symbol": symbol,
                    "interval": interval,
                    "datetime": datetime.fromtimestamp(b.ts, tz=timezone.utc),
                    "open": b.open,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": b.volume,
                }
                for b in bar_list
            ]
        )
        # DataFrame helper expects some datetime-like column
        return upsert_ohlcv(provider, symbol, interval, df)

    def fetch_range(self, *, provider: str, symbol: str, interval: str, start_ts: int, end_ts: int):
        """Fetch bars in [start_ts, end_ts]; returns list of MarketBar. Empty list on error."""
        try:
            from infrastructure.database.postgres import get_connection  # type: ignore
        except Exception:  # pragma: no cover
            return []
        sql = (
            "SELECT ts, open, high, low, close, volume FROM ohlcv "
            "WHERE source=%s AND symbol=%s AND interval=%s AND ts BETWEEN to_timestamp(%s) AND to_timestamp(%s) "
            "ORDER BY ts ASC"
        )
        rows = []
        try:
            with get_connection() as conn:  # type: ignore
                with conn.cursor() as cur:
                    cur.execute(sql, (provider, symbol, interval, start_ts, end_ts))
                    for r in cur.fetchall():
                        try:
                            rows.append(
                                MarketBar(
                                    ts=int(r[0].timestamp()),
                                    open=float(r[1]) if r[1] is not None else 0.0,
                                    high=float(r[2]) if r[2] is not None else 0.0,
                                    low=float(r[3]) if r[3] is not None else 0.0,
                                    close=float(r[4]) if r[4] is not None else 0.0,
                                    volume=float(r[5]) if r[5] is not None else 0.0,
                                    provider=provider,
                                )
                            )
                        except Exception:  # pragma: no cover
                            continue
        except Exception:  # pragma: no cover
            return []
        return rows

    def latest(self, *, provider: str, symbol: str, interval: str):
        """Fetch latest MarketBar or None."""
        try:
            from infrastructure.database.postgres import get_connection  # type: ignore
        except Exception:  # pragma: no cover
            return None
        sql = (
            "SELECT ts, open, high, low, close, volume FROM ohlcv "
            "WHERE source=%s AND symbol=%s AND interval=%s ORDER BY ts DESC LIMIT 1"
        )
        try:
            with get_connection() as conn:  # type: ignore
                with conn.cursor() as cur:
                    cur.execute(sql, (provider, symbol, interval))
                    row = cur.fetchone()
                    if not row:
                        return None
                    return MarketBar(
                        ts=int(row[0].timestamp()),
                        open=float(row[1]) if row[1] is not None else 0.0,
                        high=float(row[2]) if row[2] is not None else 0.0,
                        low=float(row[3]) if row[3] is not None else 0.0,
                        close=float(row[4]) if row[4] is not None else 0.0,
                        volume=float(row[5]) if row[5] is not None else 0.0,
                        provider=provider,
                    )
        except Exception:  # pragma: no cover
            return None
        return None


__all__ = ["to_market_bars", "BarRepository"]
