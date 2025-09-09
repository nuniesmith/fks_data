from __future__ import annotations

from adapters import get_adapter


def test_binance_adapter_normalization():
    # Fake raw 2 bars (truncated Binance format)
    fake_payload = [
        [1732646400000, "100.0", "101.0", "99.5", "100.5", "123.45", 0, 0, 0, 0, 0, 0],
        [1732646460000, "100.5", "102.0", "100.0", "101.5", "67.89", 0, 0, 0, 0, 0, 0],
    ]

    def fake_http(url, params=None, headers=None, timeout=None):  # noqa: D401
        return fake_payload

    adapter = get_adapter("binance", http=fake_http)
    out = adapter.fetch(symbol="BTCUSDT", interval="1m", limit=2)
    assert out["provider"] == "binance"
    assert len(out["data"]) == 2
    first = out["data"][0]
    assert {"ts", "open", "high", "low", "close", "volume"}.issubset(first.keys())
    assert first["open"] == 100.0
