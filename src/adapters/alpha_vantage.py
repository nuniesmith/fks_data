"""Alpha Vantage adapter using unified APIAdapter base.

Supports stocks, ETFs, and crypto data.
Free tier: 25 calls/day, 5 calls/min
Supports intraday (1min+) and daily data for stocks/ETFs.

Env vars:
  ALPHA_VANTAGE_API_KEY or FKS_ALPHA_VANTAGE_API_KEY
"""
from __future__ import annotations

import time
from typing import Any

try:
    from shared_python.exceptions import DataFetchError  # type: ignore
except (ImportError, ModuleNotFoundError):
    class DataFetchError(Exception):
        """Fallback exception when shared_python is not available."""
        pass

from .base import APIAdapter, get_env_any


class AlphaVantageAdapter(APIAdapter):
    name = "alpha_vantage"
    base_url = "https://www.alphavantage.co/query"
    rate_limit_per_sec = 0.083  # 5 calls/min = ~0.083/sec (very conservative)

    def _build_request(self, **kwargs):  # noqa: D401
        function: str = kwargs.get("function", "TIME_SERIES_INTRADAY")
        symbol: str = kwargs.get("symbol", "IBM")
        api_key = get_env_any("ALPHA_VANTAGE_API_KEY", "FKS_ALPHA_VANTAGE_API_KEY")
        
        if not api_key:
            raise DataFetchError(self.name, "ALPHA_VANTAGE_API_KEY not found in environment")
        
        params: dict[str, Any] = {
            "function": function,
            "symbol": symbol.upper(),
            "apikey": api_key,
        }
        
        if function == "TIME_SERIES_INTRADAY":
            # Intraday data for stocks/ETFs
            interval = kwargs.get("interval", "1min")
            # Valid intervals: 1min, 5min, 15min, 30min, 60min
            valid_intervals = ["1min", "5min", "15min", "30min", "60min"]
            if interval not in valid_intervals:
                interval = "1min"
            
            params["interval"] = interval
            params["outputsize"] = kwargs.get("outputsize", "full")  # "compact" or "full"
            params["datatype"] = kwargs.get("datatype", "json")  # "json" or "csv"
        
        elif function == "TIME_SERIES_DAILY":
            # Daily data
            params["outputsize"] = kwargs.get("outputsize", "full")
            params["datatype"] = kwargs.get("datatype", "json")
        
        elif function == "TIME_SERIES_DAILY_ADJUSTED":
            # Daily adjusted (includes dividends/splits)
            params["outputsize"] = kwargs.get("outputsize", "full")
            params["datatype"] = kwargs.get("datatype", "json")
        
        elif function == "DIGITAL_CURRENCY_DAILY":
            # Crypto daily data
            market = kwargs.get("market", "USD")
            params["market"] = market
        
        elif function == "CRYPTO_INTRADAY":
            # Crypto intraday (premium only, but included for completeness)
            interval = kwargs.get("interval", "1min")
            market = kwargs.get("market", "USD")
            params["interval"] = interval
            params["market"] = market
        
        else:
            raise DataFetchError(self.name, f"Unsupported function: {function}")
        
        # No special headers needed
        headers = None
        
        return self.base_url, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: D401
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        
        # Check for API errors
        if "Error Message" in raw:
            raise DataFetchError(self.name, f"API error: {raw['Error Message']}")
        if "Note" in raw:
            raise DataFetchError(self.name, f"Rate limit: {raw['Note']}")
        
        function = request_kwargs.get("function", "TIME_SERIES_INTRADAY")
        data: list[dict[str, Any]] = []
        
        # Alpha Vantage returns data in different formats based on function
        time_series_key = None
        if function == "TIME_SERIES_INTRADAY":
            interval = request_kwargs.get("interval", "1min")
            time_series_key = f"Time Series ({interval})"
        elif function == "TIME_SERIES_DAILY":
            time_series_key = "Time Series (Daily)"
        elif function == "TIME_SERIES_DAILY_ADJUSTED":
            time_series_key = "Time Series (Daily)"
        elif function == "DIGITAL_CURRENCY_DAILY":
            time_series_key = "Time Series (Digital Currency Daily)"
        elif function == "CRYPTO_INTRADAY":
            interval = request_kwargs.get("interval", "1min")
            time_series_key = f"Time Series Crypto ({interval})"
        
        if time_series_key and time_series_key in raw:
            time_series = raw[time_series_key]
            metadata = raw.get("Meta Data", {})
            symbol = metadata.get("2. Symbol") or metadata.get("1. Information", "").split()[0] or request_kwargs.get("symbol", "UNKNOWN")
            
            for timestamp_str, values in time_series.items():
                try:
                    # Parse timestamp (format varies by function)
                    if "T" in timestamp_str:
                        # ISO format: "2024-01-01 12:00:00"
                        from datetime import datetime
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                        ts = int(dt.timestamp())
                    else:
                        # Date format: "2024-01-01"
                        from datetime import datetime
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%d")
                        ts = int(dt.timestamp())
                    
                    # Extract OHLCV values (key names vary by function)
                    if function == "TIME_SERIES_DAILY_ADJUSTED":
                        open_price = float(values.get("1. open", 0))
                        high = float(values.get("2. high", 0))
                        low = float(values.get("3. low", 0))
                        close = float(values.get("4. close", 0))
                        adjusted_close = float(values.get("5. adjusted close", close))
                        volume = float(values.get("6. volume", 0))
                        dividend = float(values.get("7. dividend amount", 0))
                        split_coefficient = float(values.get("8. split coefficient", 1.0))
                    else:
                        open_price = float(values.get("1. open", values.get("1a. open (USD)", 0)))
                        high = float(values.get("2. high", values.get("2a. high (USD)", 0)))
                        low = float(values.get("3. low", values.get("3a. low (USD)", 0)))
                        close = float(values.get("4. close", values.get("4a. close (USD)", 0)))
                        volume = float(values.get("5. volume", values.get("5. volume (USD)", 0)))
                        adjusted_close = close
                        dividend = 0.0
                        split_coefficient = 1.0
                    
                    data.append(
                        {
                            "ts": ts,
                            "symbol": symbol,
                            "open": open_price,
                            "high": high,
                            "low": low,
                            "close": close,
                            "volume": volume,
                            "adjusted_close": adjusted_close,
                            "dividend": dividend,
                            "split_coefficient": split_coefficient,
                        }
                    )
                except Exception as e:
                    # Skip malformed rows
                    continue
        
        # Sort by timestamp
        data.sort(key=lambda x: x["ts"])
        
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["AlphaVantageAdapter"]

