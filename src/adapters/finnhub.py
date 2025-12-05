"""Finnhub API adapter for stock and crypto market data.

Finnhub provides:
- Real-time and historical stock data (OHLCV)
- Crypto data
- Company fundamentals
- News and sentiment
- Economic indicators

API Documentation: https://finnhub.io/docs/api
Free Tier: 60 calls/min, 1 year of intraday OHLC data
Rate Limits: 60 calls/min (free tier)

Phase 3: Quick Win - High ROI data provider addition
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import APIAdapter, DataFetchError, get_env_any

logger = logging.getLogger(__name__)


class FinnhubAdapter(APIAdapter):
    """Finnhub API adapter for market data."""

    name = "finnhub"
    base_url = "https://finnhub.io/api/v1"
    rate_limit_per_sec = 1.0  # 60 calls/min = 1 call/sec (conservative)

    # Resolution mapping: our format -> Finnhub format
    RESOLUTION_MAP = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "60m": "60",
        "1h": "60",
        "1d": "D",
        "1w": "W",
        "1M": "M",
    }

    def __init__(
        self,
        http=None,
        *,
        timeout: Optional[float] = None,
    ):
        """Initialize Finnhub adapter.

        Args:
            http: HTTP client (optional, for testing)
            timeout: Request timeout in seconds
        """
        super().__init__(http, timeout=timeout)

        # API key from environment variable
        self.api_key = get_env_any("FINNHUB_API_KEY", "FKS_FINNHUB_API_KEY")
        if not self.api_key:
            logger.warning(
                "FINNHUB_API_KEY not found in environment. "
                "Finnhub adapter will not work without API key."
            )

    def _build_request(self, **kwargs) -> tuple[str, Dict[str, Any] | None, Dict[str, str] | None]:
        """Build Finnhub API request.

        Args:
            **kwargs: Request parameters
                - symbol: Stock symbol (e.g., "AAPL")
                - resolution: Time resolution (1m, 5m, 15m, 30m, 60m, 1h, 1d, 1w, 1M)
                - from: Start timestamp (UNIX)
                - to: End timestamp (UNIX)

        Returns:
            Tuple of (url, params, headers)
        """
        if not self.api_key:
            raise DataFetchError(
                self.name,
                "FINNHUB_API_KEY not found in environment. "
                "Set FINNHUB_API_KEY or FKS_FINNHUB_API_KEY environment variable."
            )

        symbol = kwargs.get("symbol") or kwargs.get("ticker", "")
        if not symbol:
            raise DataFetchError(self.name, "symbol or ticker parameter required")

        # Get resolution
        resolution = kwargs.get("resolution", "1d")
        finnhub_resolution = self.RESOLUTION_MAP.get(resolution, "D")
        if resolution not in self.RESOLUTION_MAP:
            logger.warning(
                f"Unknown resolution '{resolution}', defaulting to 'D'. "
                f"Supported: {list(self.RESOLUTION_MAP.keys())}"
            )
            finnhub_resolution = "D"

        # Get time range
        from_ts = kwargs.get("from")
        to_ts = kwargs.get("to")

        # Convert datetime objects to UNIX timestamps if needed
        if isinstance(from_ts, datetime):
            from_ts = int(from_ts.timestamp())
        elif isinstance(from_ts, str):
            try:
                from_ts = int(datetime.fromisoformat(from_ts.replace("Z", "+00:00")).timestamp())
            except (ValueError, AttributeError):
                raise DataFetchError(self.name, f"Invalid 'from' timestamp format: {from_ts}")

        if isinstance(to_ts, datetime):
            to_ts = int(to_ts.timestamp())
        elif isinstance(to_ts, str):
            try:
                to_ts = int(datetime.fromisoformat(to_ts.replace("Z", "+00:00")).timestamp())
            except (ValueError, AttributeError):
                raise DataFetchError(self.name, f"Invalid 'to' timestamp format: {to_ts}")

        # Default to last 1 year if not specified (free tier limit)
        if not from_ts or not to_ts:
            now = int(datetime.now().timestamp())
            if not to_ts:
                to_ts = now
            if not from_ts:
                # Default to 1 year ago (free tier limit)
                from_ts = now - (365 * 24 * 60 * 60)

        # Build URL and params
        url = f"{self.base_url}/stock/candle"
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "resolution": finnhub_resolution,
            "from": from_ts,
            "to": to_ts,
            "token": self.api_key,
        }

        return url, params, None

    def _normalize(self, raw: Any, *, request_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Finnhub response to canonical format.

        Finnhub response format:
        {
            "c": [close prices],
            "h": [high prices],
            "l": [low prices],
            "o": [open prices],
            "s": "ok" or "no_data",
            "t": [timestamps],
            "v": [volumes]
        }

        Canonical format:
        {
            "provider": "finnhub",
            "data": [
                {
                    "ts": timestamp,
                    "open": float,
                    "high": float,
                    "low": float,
                    "close": float,
                    "volume": float
                },
                ...
            ]
        }

        Args:
            raw: Raw API response
            request_kwargs: Original request parameters

        Returns:
            Normalized data dictionary
        """
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"Unexpected response type: {type(raw)}")

        # Check for errors
        if raw.get("s") == "no_data":
            logger.warning(f"No data available for symbol: {request_kwargs.get('symbol')}")
            return {
                "provider": self.name,
                "data": [],
            }

        if raw.get("s") != "ok":
            error_msg = raw.get("error", f"Unknown error: {raw.get('s')}")
            raise DataFetchError(self.name, f"API error: {error_msg}")

        # Extract arrays
        timestamps = raw.get("t", [])
        opens = raw.get("o", [])
        highs = raw.get("h", [])
        lows = raw.get("l", [])
        closes = raw.get("c", [])
        volumes = raw.get("v", [])

        # Validate arrays have same length
        if not timestamps:
            logger.warning("Empty data array in Finnhub response")
            return {
                "provider": self.name,
                "data": [],
            }

        length = len(timestamps)
        if not all(len(arr) == length for arr in [opens, highs, lows, closes, volumes]):
            raise DataFetchError(
                self.name,
                f"Data arrays have mismatched lengths: "
                f"t={len(timestamps)}, o={len(opens)}, h={len(highs)}, "
                f"l={len(lows)}, c={len(closes)}, v={len(volumes)}"
            )

        # Normalize to canonical format
        normalized_data: List[Dict[str, Any]] = []
        for i in range(length):
            normalized_data.append(
                {
                    "ts": timestamps[i],
                    "open": float(opens[i]) if opens[i] is not None else None,
                    "high": float(highs[i]) if highs[i] is not None else None,
                    "low": float(lows[i]) if lows[i] is not None else None,
                    "close": float(closes[i]) if closes[i] is not None else None,
                    "volume": float(volumes[i]) if volumes[i] is not None else 0.0,
                }
            )

        return {
            "provider": self.name,
            "data": normalized_data,
        }
