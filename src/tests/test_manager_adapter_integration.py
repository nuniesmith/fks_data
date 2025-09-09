import pytest
import sys
from pathlib import Path

# Ensure project root on path so we can import manager module
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from manager import DataManager  # type: ignore  # noqa: E402


def test_manager_fetch_market_data_binance(monkeypatch):
    dm = DataManager()

    # Stub HTTP returning two kline rows (Binance format)
    sample = [
        [1700000000000, "50000", "50500", "49500", "50200", "123.4"],
        [1700000006000, "50200", "50600", "50100", "50400", "56.7"],
    ]

    def stub_http(url, params=None, headers=None, timeout=None):  # noqa: D401
        return sample

    # Patch adapter factory to inject stub http
    from adapters import get_adapter

    def factory(name: str, **kwargs):  # wrap to force stub http
        return get_adapter(name, http=stub_http)

    dm._adapter_factory = factory  # inject

    out = dm.fetch_market_data("binance", symbol="BTCUSDT", interval="1m", limit=2)
    assert out["provider"] == "binance"
    assert len(out["data"]) == 2
    assert {k for k in out["data"][0].keys()} == {"ts", "open", "high", "low", "close", "volume"}
    # Verify ordering and simple numeric parse
    assert out["data"][0]["open"] == 50000.0

