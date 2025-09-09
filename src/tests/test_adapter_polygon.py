from __future__ import annotations

from adapters import get_adapter


def test_polygon_adapter_normalization():
    fake = {
        "results": [
            {"t": 1732646400000, "o": 10.0, "h": 11.0, "l": 9.5, "c": 10.5, "v": 1000},
            {"t": 1732646460000, "o": 10.5, "h": 11.2, "l": 10.2, "c": 11.0, "v": 800},
        ]
    }

    def fake_http(url, params=None, headers=None, timeout=None):  # noqa: D401
        return fake

    adapter = get_adapter("polygon", http=fake_http)
    out = adapter.fetch(ticker="BTCUSD", range=1, timespan="day", fro="2024-01-01", to="2024-01-02")
    assert out["provider"] == "polygon"
    assert len(out["data"]) == 2
    assert out["data"][0]["open"] == 10.0