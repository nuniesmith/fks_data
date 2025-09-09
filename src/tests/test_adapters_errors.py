from __future__ import annotations

import pytest
from adapters import get_adapter
from shared_python.exceptions import DataFetchError  # type: ignore


def test_adapter_raises_datafetcherror_on_http_failure():
    def failing_http(url, params=None, headers=None, timeout=None):  # noqa: D401
        raise RuntimeError("network boom")

    adapter = get_adapter("binance", http=failing_http)
    with pytest.raises(DataFetchError):
        adapter.fetch(symbol="BTCUSDT", interval="1m", limit=1)
