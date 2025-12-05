"""Tiingo API adapter for stock and ETF market data.

Tiingo provides:
- End-of-Day (EOD) stock prices (clean, trusted by quants)
- Real-time prices
- Company fundamentals
- IEX data integration

API Documentation: https://api.tiingo.com/documentation
Free Tier: 10,000 requests/hour, 100,000 requests/day, 40GB/month bandwidth
Rate Limits: Very generous free tier

Phase 3: Quick Win - High ROI data provider addition
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import APIAdapter, DataFetchError, get_env_any

logger = logging.getLogger(__name__)


class TiingoAdapter(APIAdapter):
    """Tiingo API adapter for market data."""

    name = "tiingo"
    base_url = "https://api.tiingo.com/tiingo"
    rate_limit_per_sec = 2.0  # 10,000/hour = ~2.78/sec (conservative: 2/sec)

    # Tiingo supports daily data primarily (EOD - End of Day)
    # For intraday, they have IEX integration but it's more complex
    RESOLUTION_MAP = {
        "1d": "daily",
        "daily": "daily",
        "1w": "daily",  # Weekly via daily aggregation
        "1M": "daily",  # Monthly via daily aggregation
    }

    def __init__(
        self,
        http=None,
        *,
        timeout: Optional[float] = None,
    ):
        """Initialize Tiingo adapter.

        Args:
            http: HTTP client (optional, for testing)
            timeout: Request timeout in seconds
        """
        super().__init__(http, timeout=timeout)

        # API key from environment variable
        self.api_key = get_env_any("TIINGO_API_KEY", "FKS_TIINGO_API_KEY")
        if not self.api_key:
            logger.warning(
                "TIINGO_API_KEY not found in environment. "
                "Tiingo adapter will not work without API key."
            )

    def _build_request(self, **kwargs) -> tuple[str, Dict[str, Any] | None, Dict[str, str] | None]:
        """Build Tiingo API request.

        Args:
            **kwargs: Request parameters
                - symbol: Stock symbol (e.g., "AAPL")
                - resolution: Time resolution (1d, daily, 1w, 1M)
                - startDate: Start date (YYYY-MM-DD format)
                - endDate: End date (YYYY-MM-DD format)

        Returns:
            Tuple of (url, params, headers)
        """
        if not self.api_key:
            raise DataFetchError(
                self.name,
                "TIINGO_API_KEY not found in environment. "
                "Set TIINGO_API_KEY or FKS_TIINGO_API_KEY environment variable."
            )

        symbol = kwargs.get("symbol") or kwargs.get("ticker", "")
        if not symbol:
            raise DataFetchError(self.name, "symbol or ticker parameter required")

        # Get resolution (Tiingo primarily supports daily)
        resolution = kwargs.get("resolution", "1d")
        tiingo_freq = self.RESOLUTION_MAP.get(resolution, "daily")
        
        if resolution not in self.RESOLUTION_MAP:
            logger.warning(
                f"Unknown resolution '{resolution}', defaulting to 'daily'. "
                f"Supported: {list(self.RESOLUTION_MAP.keys())}"
            )
            tiingo_freq = "daily"

        # Get date range
        start_date = kwargs.get("startDate") or kwargs.get("from")
        end_date = kwargs.get("endDate") or kwargs.get("to")

        # Convert timestamps to date strings if needed
        if isinstance(start_date, (int, float)):
            start_date = datetime.fromtimestamp(start_date).strftime("%Y-%m-%d")
        elif isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        elif isinstance(start_date, str) and not start_date.startswith("20"):
            # Try to parse as timestamp string
            try:
                start_date = datetime.fromtimestamp(float(start_date)).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        if isinstance(end_date, (int, float)):
            end_date = datetime.fromtimestamp(end_date).strftime("%Y-%m-%d")
        elif isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        elif isinstance(end_date, str) and not end_date.startswith("20"):
            # Try to parse as timestamp string
            try:
                end_date = datetime.fromtimestamp(float(end_date)).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        # Default to last 1 year if not specified
        if not start_date or not end_date:
            now = datetime.now()
            if not end_date:
                end_date = now.strftime("%Y-%m-%d")
            if not start_date:
                # Default to 1 year ago
                from datetime import timedelta
                start_date = (now - timedelta(days=365)).strftime("%Y-%m-%d")

        # Build URL and params
        # Tiingo EOD endpoint: /tiingo/daily/{ticker}/prices
        url = f"{self.base_url}/daily/{symbol.upper()}/prices"
        params: Dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "format": "json",
        }

        # Headers with API key
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.api_key}",
        }

        return url, params, headers

    def _normalize(self, raw: Any, *, request_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Tiingo response to canonical format.

        Tiingo response format (array of objects):
        [
            {
                "date": "2024-01-01T00:00:00.000Z",
                "close": 150.0,
                "high": 152.0,
                "low": 148.0,
                "open": 149.0,
                "volume": 1000000,
                "adjClose": 150.0,
                "adjHigh": 152.0,
                "adjLow": 148.0,
                "adjOpen": 149.0,
                "adjVolume": 1000000,
                "divCash": 0.0,
                "splitFactor": 1.0
            },
            ...
        ]

        Canonical format:
        {
            "provider": "tiingo",
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
        if not isinstance(raw, list):
            raise DataFetchError(self.name, f"Unexpected response type: {type(raw)}")

        if not raw:
            logger.warning(f"No data available for symbol: {request_kwargs.get('symbol')}")
            return {
                "provider": self.name,
                "data": [],
            }

        # Normalize to canonical format
        normalized_data: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue

            # Parse date to timestamp
            date_str = item.get("date", "")
            try:
                if "T" in date_str:
                    # ISO format with time
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    # Date only
                    dt = datetime.fromisoformat(date_str)
                timestamp = int(dt.timestamp())
            except (ValueError, AttributeError, TypeError):
                logger.warning(f"Could not parse date: {date_str}")
                continue

            # Use adjusted values if available (better for historical analysis)
            close = item.get("adjClose") or item.get("close", 0)
            high = item.get("adjHigh") or item.get("high", 0)
            low = item.get("adjLow") or item.get("low", 0)
            open_price = item.get("adjOpen") or item.get("open", 0)
            volume = item.get("adjVolume") or item.get("volume", 0)

            normalized_data.append(
                {
                    "ts": timestamp,
                    "open": float(open_price) if open_price is not None else None,
                    "high": float(high) if high is not None else None,
                    "low": float(low) if low is not None else None,
                    "close": float(close) if close is not None else None,
                    "volume": float(volume) if volume is not None else 0.0,
                }
            )

        return {
            "provider": self.name,
            "data": normalized_data,
        }
