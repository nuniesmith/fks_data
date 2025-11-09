"""CoinMarketCap (CMC) adapter using unified APIAdapter base.

Supports cryptocurrency listings and quotes endpoints.
Env vars:
  CMC_API_KEY or FKS_CMC_API_KEY for auth header.
"""
from __future__ import annotations

import os
import time
from typing import Any

try:
    from shared_python.exceptions import DataFetchError  # type: ignore
except (ImportError, ModuleNotFoundError):
    class DataFetchError(Exception):
        """Fallback exception when shared_python is not available."""
        pass

from .base import APIAdapter, get_env_any


class CoinMarketCapAdapter(APIAdapter):
    name = "cmc"
    base_url = "https://pro-api.coinmarketcap.com/v1"
    rate_limit_per_sec = 0.5  # 30 calls/min = 0.5/sec (conservative)

    def _build_request(self, **kwargs):  # noqa: D401
        endpoint: str = kwargs.get("endpoint", "listings_latest")
        api_key = get_env_any("CMC_API_KEY", "FKS_CMC_API_KEY")
        
        if not api_key:
            raise DataFetchError(self.name, "CMC_API_KEY not found in environment")
        
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": api_key,
        }
        
        if endpoint == "listings_latest":
            path = "/cryptocurrency/listings/latest"
            limit = int(kwargs.get("limit", 100))
            params = {
                "start": kwargs.get("start", 1),
                "limit": limit,
                "convert": kwargs.get("convert", "USD"),
            }
        elif endpoint == "quotes_latest":
            path = "/cryptocurrency/quotes/latest"
            symbol = kwargs.get("symbol", "BTC")
            params = {
                "symbol": symbol.upper(),
                "convert": kwargs.get("convert", "USD"),
            }
        elif endpoint == "market_chart":
            path = "/cryptocurrency/market-chart"
            symbol = kwargs.get("symbol", "BTC")
            params = {
                "symbol": symbol.upper(),
                "convert": kwargs.get("convert", "USD"),
                "interval": kwargs.get("interval", "daily"),
            }
        else:
            raise DataFetchError(self.name, f"Unknown endpoint: {endpoint}")
        
        return self.base_url + path, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: D401
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        
        endpoint = request_kwargs.get("endpoint", "listings_latest")
        data: list[dict[str, Any]] = []
        
        if endpoint == "listings_latest":
            results = raw.get("data", [])
            for item in results:
                try:
                    quote = item.get("quote", {}).get("USD", {})
                    # Parse ISO timestamp to unix seconds
                    last_updated = item.get("last_updated", "")
                    ts = 0
                    if last_updated:
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                            ts = int(dt.timestamp())
                        except Exception:
                            ts = int(time.time())  # Fallback to current time
                    data.append(
                        {
                            "ts": ts,
                            "symbol": item.get("symbol", ""),
                            "name": item.get("name", ""),
                            "price": float(quote.get("price", 0)),
                            "volume_24h": float(quote.get("volume_24h", 0)),
                            "market_cap": float(quote.get("market_cap", 0)),
                            "percent_change_24h": float(quote.get("percent_change_24h", 0)),
                            # Normalize to OHLCV format for compatibility
                            "open": float(quote.get("price", 0)),  # Use current price as open
                            "high": float(quote.get("price", 0)),  # Use current price as high
                            "low": float(quote.get("price", 0)),    # Use current price as low
                            "close": float(quote.get("price", 0)),  # Use current price as close
                            "volume": float(quote.get("volume_24h", 0)),
                        }
                    )
                except Exception:
                    continue
        elif endpoint == "quotes_latest":
            data_dict = raw.get("data", {})
            for symbol, item in data_dict.items():
                try:
                    quote = item.get("quote", {}).get("USD", {})
                    # Parse ISO timestamp to unix seconds
                    last_updated = item.get("last_updated", "")
                    ts = 0
                    if last_updated:
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                            ts = int(dt.timestamp())
                        except Exception:
                            ts = int(time.time())  # Fallback to current time
                    data.append(
                        {
                            "ts": ts,
                            "symbol": item.get("symbol", symbol),
                            "price": float(quote.get("price", 0)),
                            "volume_24h": float(quote.get("volume_24h", 0)),
                            "market_cap": float(quote.get("market_cap", 0)),
                            "percent_change_24h": float(quote.get("percent_change_24h", 0)),
                            # Normalize to OHLCV format
                            "open": float(quote.get("price", 0)),
                            "high": float(quote.get("price", 0)),
                            "low": float(quote.get("price", 0)),
                            "close": float(quote.get("price", 0)),
                            "volume": float(quote.get("volume_24h", 0)),
                        }
                    )
                except Exception:
                    continue
        
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["CoinMarketCapAdapter"]

