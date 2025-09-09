"""Legacy-style Binance provider function now delegating to new adapter layer.

This keeps backward compatibility for code that still imports the provider
function while steering logic through the unified `APIAdapter` implementation.
Will be removed after call sites migrate to `BinanceAdapter` directly.
"""
from __future__ import annotations
from typing import Any, Dict, List, Callable

try:
    from adapters import get_adapter  # type: ignore
except Exception:  # pragma: no cover
    get_adapter = None  # type: ignore


def binance_klines(requester: Callable[[str, Dict[str, Any]], List[Any]] | None, symbol: str, interval: str, limit: int, start_time: str | None, end_time: str | None) -> Dict[str, Any]:  # noqa: D401,E501
    if get_adapter is None:  # Fallback to prior inlined logic (shouldn't happen post Week 2)
        base = "https://fapi.binance.com"
        path = "/fapi/v1/klines"
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time: params["startTime"] = start_time
        if end_time: params["endTime"] = end_time
        klines = requester(base + path, params) if requester else []
        data = []
        for k in klines:
            data.append({
                "time": int(k[0] // 1000),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            })
        return {"data": data}

    # Delegate to adapter (adapter returns 'data' with 'ts' key; preserve legacy 'time')
    adapter = get_adapter("binance", http=(lambda url, params=None, headers=None, timeout=None: requester(url, params) if requester else []))  # type: ignore[arg-type]
    result = adapter.fetch(symbol=symbol, interval=interval, limit=limit, start_time=start_time, end_time=end_time)
    # Map ts -> time for backward compatibility
    legacy_rows = []
    for row in result["data"]:
        row_copy = dict(row)
        row_copy["time"] = row_copy.pop("ts", None)
        legacy_rows.append(row_copy)
    return {"data": legacy_rows, "provider": result.get("provider")}
