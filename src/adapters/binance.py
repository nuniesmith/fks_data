"""Binance adapter (Futures/Spot klines minimal) built on Week 2 scaffolding."""
from __future__ import annotations

from typing import Any, Dict, List
from .base import APIAdapter, get_env_any
from shared_python.exceptions import DataFetchError  # type: ignore


class BinanceAdapter(APIAdapter):
    name = "binance"
    base_url = "https://fapi.binance.com"
    rate_limit_per_sec = 10  # conservative (Binance allows more, we keep low)

    def _build_request(self, **kwargs):  # noqa: D401
        symbol: str = kwargs.get("symbol", "BTCUSDT")
        interval: str = kwargs.get("interval", "1m")
        limit: int = int(kwargs.get("limit", 500))
        start_time = kwargs.get("start_time")
        end_time = kwargs.get("end_time")
        path = "/fapi/v1/klines"
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        # Binance public klines need no auth; placeholder for future API key usage
        headers: Dict[str, str] | None = None
        return self.base_url + path, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: Dict[str, Any]) -> Dict[str, Any]:  # noqa: D401
        if not isinstance(raw, list):  # Unexpected shape
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        data: List[Dict[str, Any]] = []
        for item in raw:
            # Official format: [ openTime, open, high, low, close, volume, closeTime, ... ]
            try:
                data.append(
                    {
                        "ts": int(item[0] // 1000),
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                    }
                )
            except Exception as e:  # pragma: no cover - skip malformed row
                continue
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["BinanceAdapter"]
