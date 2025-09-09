"""Legacy Polygon provider delegating to PolygonAdapter (Week 2 migration)."""
from __future__ import annotations
from typing import Any, Dict, List, Callable
import warnings

try:
    from adapters import get_adapter  # type: ignore
except Exception:  # pragma: no cover
    get_adapter = None  # type: ignore


def polygon_aggs(requester: Callable[[str, Dict[str, Any]], Dict[str, Any]] | None, ticker: str, rng: str, timespan: str, fro: str, to: str) -> Dict[str, Any]:  # noqa: D401,E501
    warnings.warn("polygon_aggs legacy function will be removed; use PolygonAdapter", DeprecationWarning, stacklevel=2)
    if get_adapter is None:  # fallback path (unlikely after migration)
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{rng}/{timespan}/{fro}/{to}"
        params = {"adjusted": "true", "sort": "asc", "limit": 50000}
        j = requester(url, params) if requester else {"results": []}
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
    adapter = get_adapter("polygon", http=(lambda url, params=None, headers=None, timeout=None: requester(url, params) if requester else {"results": []}))  # type: ignore[arg-type]
    result = adapter.fetch(ticker=ticker, range=rng, timespan=timespan, fro=fro, to=to)
    legacy_rows = []
    for row in result["data"]:
        row_copy = dict(row)
        row_copy["time"] = row_copy.pop("ts", None)
        legacy_rows.append(row_copy)
    return {"data": legacy_rows, "provider": result.get("provider")}

