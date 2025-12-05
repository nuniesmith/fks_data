"""pandas-datareader adapter for macro economic data.

pandas-datareader provides access to:
- FRED (Federal Reserve Economic Data)
- World Bank indicators
- OECD data
- Eurostat data
- Yahoo Finance (fallback)

API Documentation: https://pandas-datareader.readthedocs.io/
Free Tier: No limits (public data sources)
Rate Limits: Varies by source (FRED: generous, World Bank: generous)

Phase 3: Quick Win - Macro/FRED/World Bank data addition
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .base import APIAdapter, DataFetchError

logger = logging.getLogger(__name__)

# Try to import pandas-datareader
try:
    import pandas_datareader.data as web
    import pandas as pd
    HAS_PANDAS_DATAREADER = True
except ImportError:
    HAS_PANDAS_DATAREADER = False
    logger.warning("pandas-datareader not installed. Install with: pip install pandas-datareader")


class PandasDataReaderAdapter(APIAdapter):
    """pandas-datareader adapter for macro economic data."""

    name = "pandas_datareader"
    base_url = "pandas-datareader"  # Not a URL, but identifier
    rate_limit_per_sec = 1.0  # Conservative rate limiting

    # Data source mapping
    SOURCE_MAP = {
        "fred": "fred",
        "FRED": "fred",
        "worldbank": "world_bank",
        "wb": "world_bank",
        "oecd": "oecd",
        "eurostat": "eurostat",
        "yahoo": "yahoo",  # Fallback
    }

    def __init__(
        self,
        http=None,
        *,
        timeout: Optional[float] = None,
    ):
        """Initialize pandas-datareader adapter.

        Args:
            http: HTTP client (not used, but required by base class)
            timeout: Request timeout in seconds
        """
        super().__init__(http, timeout=timeout)

        # Don't raise error here - let fetch() handle it
        # This allows the adapter to be registered even if library isn't installed
        if not HAS_PANDAS_DATAREADER:
            logger.warning(
                "pandas-datareader not installed. "
                "Install with: pip install pandas-datareader. "
                "Adapter will not work until installed."
            )

    def _build_request(self, **kwargs) -> tuple[str, Dict[str, Any] | None, Dict[str, str] | None]:
        """Build pandas-datareader request parameters.

        Args:
            **kwargs: Request parameters
                - symbol: Data symbol (e.g., "GDP" for FRED, "AAPL" for Yahoo)
                - source: Data source (fred, worldbank, oecd, eurostat, yahoo)
                - start: Start date (datetime or string)
                - end: End date (datetime or string)

        Returns:
            Tuple of (source, params, None) - source is the data source name
        """
        symbol = kwargs.get("symbol") or kwargs.get("ticker", "")
        if not symbol:
            raise DataFetchError(self.name, "symbol or ticker parameter required")

        # Get data source
        source = kwargs.get("source", "fred").lower()
        source = self.SOURCE_MAP.get(source, "fred")

        # Get date range
        start_date = kwargs.get("start") or kwargs.get("startDate")
        end_date = kwargs.get("end") or kwargs.get("endDate")

        # Convert to datetime if needed
        if isinstance(start_date, str):
            try:
                start_date = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                try:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")
                except ValueError:
                    raise DataFetchError(self.name, f"Invalid start date format: {start_date}")
        elif isinstance(start_date, (int, float)):
            start_date = datetime.fromtimestamp(start_date)

        if isinstance(end_date, str):
            try:
                end_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                try:
                    end_date = datetime.strptime(end_date, "%Y-%m-%d")
                except ValueError:
                    raise DataFetchError(self.name, f"Invalid end date format: {end_date}")
        elif isinstance(end_date, (int, float)):
            end_date = datetime.fromtimestamp(end_date)

        # Default to last 1 year if not specified
        if not start_date or not end_date:
            now = datetime.now()
            if not end_date:
                end_date = now
            if not start_date:
                start_date = now - timedelta(days=365)

        # Build params dict
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "source": source,
            "start": start_date,
            "end": end_date,
        }

        return source, params, None

    def _normalize(self, raw: Any, *, request_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize pandas-datareader response to canonical format.

        pandas-datareader returns a pandas DataFrame with:
        - Index: DatetimeIndex
        - Columns: Data values (e.g., 'Close', 'Value', etc.)

        Canonical format:
        {
            "provider": "pandas_datareader",
            "data": [
                {
                    "ts": timestamp,
                    "value": float  # or "close", "open", etc.
                },
                ...
            ]
        }

        Args:
            raw: Raw pandas DataFrame
            request_kwargs: Original request parameters

        Returns:
            Normalized data dictionary
        """
        if not HAS_PANDAS_DATAREADER:
            raise DataFetchError(self.name, "pandas-datareader not available")

        if raw is None or raw.empty:
            logger.warning(f"No data available for symbol: {request_kwargs.get('symbol')}")
            return {
                "provider": self.name,
                "data": [],
            }

        # Convert DataFrame to list of dicts
        normalized_data: List[Dict[str, Any]] = []

        # Determine value column (prefer 'Close', then 'Value', then first numeric column)
        value_column = None
        for col in ["Close", "Value", "close", "value"]:
            if col in raw.columns:
                value_column = col
                break

        if value_column is None:
            # Use first numeric column
            numeric_cols = raw.select_dtypes(include=["float64", "int64"]).columns
            if len(numeric_cols) > 0:
                value_column = numeric_cols[0]
            else:
                raise DataFetchError(self.name, "No numeric columns found in data")

        # Convert each row
        for idx, row in raw.iterrows():
            # Convert index (datetime) to timestamp
            if isinstance(idx, pd.Timestamp):
                timestamp = int(idx.timestamp())
            elif isinstance(idx, datetime):
                timestamp = int(idx.timestamp())
            else:
                try:
                    timestamp = int(pd.to_datetime(idx).timestamp())
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse timestamp: {idx}")
                    continue

            value = row.get(value_column)
            if pd.isna(value):
                continue

            normalized_data.append(
                {
                    "ts": timestamp,
                    "value": float(value),
                    # Include other columns if available
                    "open": float(row.get("Open", value)) if "Open" in row else None,
                    "high": float(row.get("High", value)) if "High" in row else None,
                    "low": float(row.get("Low", value)) if "Low" in row else None,
                    "close": float(row.get("Close", value)) if "Close" in row else None,
                    "volume": float(row.get("Volume", 0)) if "Volume" in row else 0.0,
                }
            )

        return {
            "provider": self.name,
            "data": normalized_data,
        }

    def fetch(self, **kwargs) -> dict[str, Any]:
        """Fetch data using pandas-datareader.

        Override base fetch to handle pandas-datareader's different API.
        """
        if not HAS_PANDAS_DATAREADER:
            raise DataFetchError(
                self.name,
                "pandas-datareader not installed. Install with: pip install pandas-datareader"
            )

        self._respect_rate_limit()

        try:
            source, params, _ = self._build_request(**kwargs)

            symbol = params["symbol"]
            start = params["start"]
            end = params["end"]

            self._log.debug("request", extra={"symbol": symbol, "source": source, "start": start, "end": end})

            # Fetch data using pandas-datareader
            if source == "fred":
                # FRED data
                raw = web.DataReader(symbol, source, start, end)
            elif source == "world_bank":
                # World Bank data (requires indicator code)
                # For now, try as FRED-like symbol
                raw = web.DataReader(symbol, "fred", start, end)
            else:
                # Other sources (yahoo, oecd, etc.)
                raw = web.DataReader(symbol, source, start, end)

            normalized = self._normalize(raw, request_kwargs=kwargs)
            self._log.info("fetched", extra={"rows": len(normalized.get("data", [])), "status": "ok"})
            return normalized

        except Exception as e:
            self._log.error("fetch_failed", extra={"error": str(e)})
            raise DataFetchError(self.name, str(e)) from e
