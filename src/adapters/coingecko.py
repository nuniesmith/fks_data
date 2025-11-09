"""CoinGecko adapter using unified APIAdapter base.

Supports cryptocurrency market data with automatic interval selection:
- Hourly data for 1-90 days
- Daily data for >90 days or max range
- No minute-level data (not supported by free tier)

Env vars: No API key required for free tier (500 calls/min)
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

try:
    from shared_python.exceptions import DataFetchError  # type: ignore
except (ImportError, ModuleNotFoundError):
    class DataFetchError(Exception):
        """Fallback exception when shared_python is not available."""
        pass

from .base import APIAdapter


class CoinGeckoAdapter(APIAdapter):
    name = "coingecko"
    base_url = "https://api.coingecko.com/api/v3"
    rate_limit_per_sec = 8.33  # 500 calls/min = ~8.33/sec (conservative)

    def _build_request(self, **kwargs):  # noqa: D401
        endpoint: str = kwargs.get("endpoint", "market_chart")
        coin_id: str = kwargs.get("coin_id") or kwargs.get("symbol", "bitcoin")
        vs_currency: str = kwargs.get("vs_currency", "usd")
        
        # CoinGecko uses coin IDs (e.g., "bitcoin", "ethereum") not symbols
        # Map common symbols to IDs
        symbol_to_id = {
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "BNB": "binancecoin",
            "SOL": "solana",
            "ADA": "cardano",
            "XRP": "ripple",
            "DOT": "polkadot",
            "DOGE": "dogecoin",
            "AVAX": "avalanche-2",
            "MATIC": "matic-network",
        }
        
        if coin_id.upper() in symbol_to_id:
            coin_id = symbol_to_id[coin_id.upper()]
        
        if endpoint == "market_chart":
            path = f"/coins/{coin_id}/market_chart"
            days = kwargs.get("days", 1)
            # Auto-select interval: hourly for <=90 days, daily for >90 days
            interval = kwargs.get("interval", "hourly" if days <= 90 else "daily")
            params = {
                "vs_currency": vs_currency,
                "days": days,
                "interval": interval,
            }
        elif endpoint == "simple_price":
            path = "/simple/price"
            # Convert coin_id to symbol for simple_price endpoint
            symbol = coin_id.upper() if coin_id.upper() in symbol_to_id else coin_id
            params = {
                "ids": coin_id,
                "vs_currencies": vs_currency,
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_24hr_change": "true",
            }
        elif endpoint == "coins_list":
            path = "/coins/list"
            params = {"include_platform": "false"}
        else:
            raise DataFetchError(self.name, f"Unknown endpoint: {endpoint}")
        
        # No API key required for free tier
        headers = None
        
        return self.base_url + path, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: D401
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        
        endpoint = request_kwargs.get("endpoint", "market_chart")
        data: list[dict[str, Any]] = []
        
        if endpoint == "market_chart":
            # CoinGecko returns: {"prices": [[timestamp_ms, price], ...], "market_caps": [...], "total_volumes": [...]}
            prices = raw.get("prices", [])
            market_caps = raw.get("market_caps", [])
            volumes = raw.get("total_volumes", [])
            
            # Create a map of timestamps to data points
            price_map = {int(p[0] // 1000): {"price": float(p[1])} for p in prices}
            market_cap_map = {int(m[0] // 1000): {"market_cap": float(m[1])} for m in market_caps}
            volume_map = {int(v[0] // 1000): {"volume": float(v[1])} for v in volumes}
            
            # Combine all timestamps
            all_timestamps = set(price_map.keys()) | set(market_cap_map.keys()) | set(volume_map.keys())
            
            for ts in sorted(all_timestamps):
                price_data = price_map.get(ts, {})
                market_cap_data = market_cap_map.get(ts, {})
                volume_data = volume_map.get(ts, {})
                
                price = price_data.get("price", 0)
                data.append(
                    {
                        "ts": ts,
                        "open": price,  # CoinGecko only provides price, use as OHLC
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": volume_data.get("volume", 0),
                        "market_cap": market_cap_data.get("market_cap", 0),
                    }
                )
        
        elif endpoint == "simple_price":
            # CoinGecko returns: {"bitcoin": {"usd": 50000, "usd_market_cap": 1000000000, ...}}
            for coin_id, price_data in raw.items():
                price = float(price_data.get(vs_currency := request_kwargs.get("vs_currency", "usd"), 0))
                market_cap = float(price_data.get(f"{vs_currency}_market_cap", 0))
                volume_24h = float(price_data.get(f"{vs_currency}_24h_vol", 0))
                change_24h = float(price_data.get(f"{vs_currency}_24h_change", 0))
                
                data.append(
                    {
                        "ts": int(time.time()),
                        "symbol": coin_id,
                        "price": price,
                        "market_cap": market_cap,
                        "volume_24h": volume_24h,
                        "percent_change_24h": change_24h,
                        # Normalize to OHLCV format
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": volume_24h,
                    }
                )
        
        elif endpoint == "coins_list":
            # Return list of available coins
            coins = raw if isinstance(raw, list) else []
            for coin in coins:
                data.append(
                    {
                        "id": coin.get("id", ""),
                        "symbol": coin.get("symbol", ""),
                        "name": coin.get("name", ""),
                    }
                )
        
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["CoinGeckoAdapter"]

