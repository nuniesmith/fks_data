"""Polygon adapter using unified APIAdapter base.

Supports aggregate bars endpoint.
Env vars:
  POLYGON_API_KEY or FKS_POLYGON_API_KEY for auth header.
"""
from __future__ import annotations

from typing import Any, Dict, List
import os
from .base import APIAdapter, get_env_any
from shared_python.exceptions import DataFetchError  # type: ignore


class PolygonAdapter(APIAdapter):
    name = "polygon"
    base_url = "https://api.polygon.io"
    rate_limit_per_sec = 4  # conservative default

    def _build_request(self, **kwargs):  # noqa: D401
        ticker: str = kwargs["ticker"]
        rng: int = int(kwargs.get("range", 1))
        timespan: str = kwargs.get("timespan", "day")
        fro: str = kwargs["fro"]
        to: str = kwargs["to"]
        path = f"/v2/aggs/ticker/{ticker}/range/{rng}/{timespan}/{fro}/{to}"
        params: Dict[str, Any] = {"adjusted": "true", "sort": "asc", "limit": 50000}
        api_key = get_env_any("POLYGON_API_KEY", "FKS_POLYGON_API_KEY")
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
        return self.base_url + path, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: Dict[str, Any]) -> Dict[str, Any]:  # noqa: D401
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        results = raw.get("results") or []
        data: List[Dict[str, Any]] = []
        for item in results:
            try:
                data.append(
                    {
                        "ts": int(item.get("t", 0) // 1000),
                        "open": float(item.get("o", 0)),
                        "high": float(item.get("h", 0)),
                        "low": float(item.get("l", 0)),
                        "close": float(item.get("c", 0)),
                        "volume": float(item.get("v", 0)),
                    }
                )
            except Exception:
                continue
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["PolygonAdapter"]
