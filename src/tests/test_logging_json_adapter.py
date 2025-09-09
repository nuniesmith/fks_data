from __future__ import annotations

import os
import json
from io import StringIO
import logging

from adapters import get_adapter


def test_adapter_emits_json_logs(monkeypatch):
    monkeypatch.setenv("FKS_JSON_LOGS", "1")
    # Capture logs
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    # Force re-init logging (prefer canonical)
    try:
        from shared_python import logging as shared_logging  # type: ignore
    except Exception:  # pragma: no cover
        from shared_python import logging as shared_logging  # type: ignore
    shared_logging._LOGGER_INITIALIZED = False  # type: ignore
    shared_logging.init_logging(force=True)
    root = logging.getLogger()
    # Ensure JSON formatter applied
    try:
        from shared_python.logging import _JsonFormatter  # type: ignore
    except Exception:  # pragma: no cover
        from shared_python.logging import _JsonFormatter  # type: ignore
    handler.setFormatter(_JsonFormatter())
    root.handlers = [handler]

    def fake_http(url, params=None, headers=None, timeout=None):
        return [[1732646400000, "100.0", "101.0", "99.5", "100.5", "123.45", 0, 0, 0, 0, 0, 0]]

    adapter = get_adapter("binance", http=fake_http)
    out = adapter.fetch(symbol="BTCUSDT", interval="1m", limit=1)
    assert out["provider"] == "binance"
    raw_lines = stream.getvalue().strip().splitlines()
    assert raw_lines, "no logs captured"
    json_lines = []
    for line in raw_lines:
        try:
            json_lines.append(json.loads(line))
        except Exception:
            continue
    assert json_lines, f"no JSON lines captured: {raw_lines}"
    # At least one line should represent request or fetched event
    msgs = {j.get("msg") for j in json_lines}
    assert {"fetched", "request"} & msgs