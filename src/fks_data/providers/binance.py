"""Binance Futures provider extraction."""
from __future__ import annotations
from typing import Any, Dict, List, Callable


def binance_klines(requester: Callable[[str, Dict[str, Any]], List[Any]], symbol: str, interval: str, limit: int, start_time: str | None, end_time: str | None) -> Dict[str, Any]:
    base = "https://fapi.binance.com"
    path = "/fapi/v1/klines"
    params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time: params["startTime"] = start_time
    if end_time: params["endTime"] = end_time
    klines = requester(base + path, params)
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
