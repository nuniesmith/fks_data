import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bars import to_market_bars  # type: ignore


def test_to_market_bars_validates_schema():
    adapter_out = {
        "provider": "binance",
        "data": [
            {"ts": 1700000000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0},
            {"ts": 1700000060, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 11.0},
        ],
    }
    bars = to_market_bars(adapter_out, validate=True)
    assert len(bars) == 2


def test_to_market_bars_invalid_row_skipped():
    adapter_out = {
        "provider": "binance",
        "data": [
            {"ts": 1700000000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0},
            {"ts": "bad", "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 11.0},
        ],
    }
    bars = to_market_bars(adapter_out, validate=True)
    assert len(bars) == 1
