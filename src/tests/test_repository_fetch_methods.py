import sys
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timezone, timedelta

# Ensure src root is on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bars import BarRepository, MarketBar  # type: ignore


def test_fetch_range_and_latest_with_stub(monkeypatch):
    # Build stub rows
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    raw_rows = []
    for i in range(3):
        ts = base + timedelta(minutes=i)
        raw_rows.append(
            (
                ts,
                100.0 + i,
                101.0 + i,
                99.0 + i,
                100.5 + i,
                10.0 + i,
            )
        )

    # Cursor stub
    class Cursor:
        def __init__(self):
            self._executed = []
        def execute(self, sql, params):
            self._executed.append((sql, params))
        def fetchall(self):
            return raw_rows
        def fetchone(self):
            return raw_rows[-1]
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            return False

    class Conn:
        def cursor(self):
            return Cursor()
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            return False

    def fake_get_connection():
        return Conn()

    monkeypatch.setitem(sys.modules, 'infrastructure.database.postgres', SimpleNamespace(get_connection=lambda: fake_get_connection()))

    repo = BarRepository()

    start = int(base.timestamp())
    end = int((base + timedelta(minutes=2)).timestamp())
    out = repo.fetch_range(provider="binance", symbol="BTCUSDT", interval="1m", start_ts=start, end_ts=end)
    assert len(out) == 3
    assert out[0].open == 100.0
    last = repo.latest(provider="binance", symbol="BTCUSDT", interval="1m")
    assert last is not None
    assert last.ts == int(raw_rows[-1][0].timestamp())
