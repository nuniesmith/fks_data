from __future__ import annotations

import os
import time
from adapters import get_adapter


def test_retry_backoff(monkeypatch):
    # Ensure deterministic backoff by fixing random
    monkeypatch.setenv("FKS_API_MAX_RETRIES", "2")  # total attempts = 3
    monkeypatch.setenv("FKS_API_BACKOFF_BASE", "0.0")  # no base delay
    monkeypatch.setenv("FKS_API_BACKOFF_JITTER", "0.0")  # no jitter
    calls = {"n": 0}

    def flaky(url, params=None, headers=None, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return [[1732646400000, "100", "101", "99", "100", "1", 0, 0, 0, 0, 0, 0]]

    # Patch sleep to avoid slow test
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    adapter = get_adapter("binance", http=flaky)
    out = adapter.fetch(symbol="BTCUSDT", interval="1m", limit=1)
    assert out["provider"] == "binance"
    assert calls["n"] == 3  # 2 failures + 1 success


def test_retry_exhaust(monkeypatch):
    monkeypatch.setenv("FKS_API_MAX_RETRIES", "1")  # attempts=2
    monkeypatch.setenv("FKS_API_BACKOFF_BASE", "0.0")
    monkeypatch.setenv("FKS_API_BACKOFF_JITTER", "0.0")
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    calls = {"n": 0}

    def always_fail(url, params=None, headers=None, timeout=None):
        calls["n"] += 1
        raise RuntimeError("down")

    adapter = get_adapter("binance", http=always_fail)
    from shared_python.exceptions import DataFetchError  # type: ignore

    try:
        adapter.fetch(symbol="BTCUSDT", interval="1m", limit=1)
        assert False, "expected DataFetchError"
    except DataFetchError as e:
        assert "failed after" in str(e)
    assert calls["n"] == 2