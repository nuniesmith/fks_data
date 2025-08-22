"""Polygon provider extraction."""
from __future__ import annotations
from typing import Any, Dict, List, Callable


def polygon_aggs(requester: Callable[[str, Dict[str, Any]], Dict[str, Any]], ticker: str, rng: str, timespan: str, fro: str, to: str) -> Dict[str, Any]:
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{rng}/{timespan}/{fro}/{to}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    j = requester(url, params)
    results = j.get("results") or []
    data: List[Dict[str, Any]] = []
    for item in results:
        data.append({
            "time": int(item.get("t", 0) // 1000),
            "open": float(item.get("o", 0)),
            "high": float(item.get("h", 0)),
            "low": float(item.get("l", 0)),
            "close": float(item.get("c", 0)),
            "volume": float(item.get("v", 0)),
        })
    return {"data": data}
