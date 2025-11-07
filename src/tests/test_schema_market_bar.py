import json
from pathlib import Path

import pytest

try:
    import jsonschema  # type: ignore
except ImportError:  # pragma: no cover
    pytest.skip("jsonschema not installed", allow_module_level=True)

from adapters import get_adapter  # type: ignore


def test_market_bar_schema_validation(monkeypatch):
    # Load schema
    root = Path(__file__).resolve().parents[3]
    primary = root / "shared" / "shared_schema" / "market_bar.schema.json"
    fallback = root / "data" / "shared" / "shared_schema" / "market_bar.schema.json"
    path = primary if primary.exists() else fallback
    with open(path) as f:
        schema = json.load(f)

    # Stub HTTP returning one kline row
    sample = [
        [1700000000000, "50000", "50500", "49500", "50200", "123.4"],
    ]

    def stub_http(url, params=None, headers=None, timeout=None):
        return sample

    adapter = get_adapter("binance", http=stub_http)
    out = adapter.fetch(symbol="BTCUSDT", interval="1m", limit=1)
    assert out["data"], "Expected at least one normalized bar"
    bar = out["data"][0]
    # Validate each bar; ensure only allowed keys
    jsonschema.validate(instance=bar, schema=schema)
