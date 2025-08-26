"""
Candle (OHLCV) data model representing price action over a specific time interval.

This module provides a standardized representation of candlestick data
across all data sources and exchanges in the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Union

# Try to use our centralized logging, fall back to loguru if not available
try:
    from app.trading.logging import get_logger

    logger = get_logger("data.models.candle")
except ImportError:
    try:
        from loguru import logger
    except ImportError:
        import logging

        logger = logging.getLogger("data.models.candle")
        logger.setLevel(logging.INFO)

# Import base Model class, fall back to a simple implementation if not available
try:
    from core.models.base import BaseDataModel

    # Define the base class to use
    ModelBase = BaseDataModel
except ImportError:
    logger.debug("core.model.Model not available, using basic object as base class")

    class ModelBase:
        """Simple base class fallback"""

        pass


# Import TimeInterval, fall back to string if not available
try:
    from core.types.market import TimeInterval

    IntervalType = Union[str, TimeInterval]
except ImportError:
    logger.debug("core.types.market.TimeInterval not available, using str as fallback")
    IntervalType = str


@dataclass
class Candle(ModelBase):
    """
    Represents candlestick (OHLCV) data for a trading pair over a specific time interval.

    Attributes:
        symbol: The trading pair or ticker symbol (e.g., 'BTC/USD')
        timestamp: The opening timestamp of the candle
        open: The opening price
        high: The highest price during the interval
        low: The lowest price during the interval
        close: The closing price
        volume: The trading volume during the interval
        interval: The time interval represented by this candle (e.g., '1m', '1h', '1d')
        trades: Optional number of trades executed during the interval
        source: Data source that provided this candle
        metadata: Additional data source specific information
    """

    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    interval: Union[str, "TimeInterval"] = "1h"
    trades: Optional[int] = None
    source: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate candle data after initialization"""
        # Ensure high is the highest price
        self.high = max(self.open, self.high, self.low, self.close)

        # Ensure low is the lowest price
        self.low = min(self.open, self.high, self.low, self.close)

        # Ensure volume is non-negative
        self.volume = max(0, self.volume)

        # If interval is a string and TimeInterval is available, try to convert
        if isinstance(self.interval, str) and "TimeInterval" in globals():
            try:
                self.interval = TimeInterval.from_string(self.interval)
            except (ValueError, AttributeError):
                # Keep as string if conversion fails
                pass

    @property
    def price_range(self) -> float:
        """
        Calculate the price range of the candle.

        Returns:
            The difference between high and low prices
        """
        return self.high - self.low

    @property
    def body_size(self) -> float:
        """
        Calculate the body size of the candle.

        Returns:
            The absolute difference between open and close prices
        """
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        """
        Determine if the candle is bullish (close > open).

        Returns:
            True if the candle is bullish, False otherwise
        """
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """
        Determine if the candle is bearish (close < open).

        Returns:
            True if the candle is bearish, False otherwise
        """
        return self.close < self.open

    @property
    def is_doji(self) -> bool:
        """
        Determine if the candle is a doji (open ≈ close).

        Returns:
            True if the candle is a doji, False otherwise
        """
        # Consider it a doji if body is less than 5% of the range
        if self.price_range == 0:
            return True
        return (self.body_size / self.price_range) < 0.05

    @property
    def upper_shadow(self) -> float:
        """
        Calculate the length of the upper shadow (wick).

        Returns:
            The length of the upper shadow
        """
        return self.high - max(self.open, self.close)

    @property
    def lower_shadow(self) -> float:
        """
        Calculate the length of the lower shadow (wick).

        Returns:
            The length of the lower shadow
        """
        return min(self.open, self.close) - self.low

    @property
    def range(self) -> float:
        """Alias for price_range for backward compatibility"""
        return self.price_range

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the candle to a dictionary.

        Returns:
            Dictionary representation of the candle
        """
        result = {
            "symbol": self.symbol,
            "timestamp": (
                self.timestamp.isoformat()
                if isinstance(self.timestamp, datetime)
                else self.timestamp
            ),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "interval": str(self.interval),
            "source": self.source,
        }

        # Add optional fields
        if self.trades is not None:
            result["trades"] = self.trades

        # Add metadata
        result.update(self.metadata)

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Candle":
        """
        Create a Candle instance from a dictionary.

        Args:
            data: Dictionary containing candle data

        Returns:
            A new Candle instance
        """
        # Make a copy to avoid modifying the original
        data_copy = data.copy()

        # Extract the core fields
        core_fields = [
            "symbol",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "interval",
            "trades",
            "source",
        ]

        # Convert timestamp if it's a string
        if "timestamp" in data_copy and isinstance(data_copy["timestamp"], str):
            try:
                data_copy["timestamp"] = datetime.fromisoformat(data_copy["timestamp"])
            except ValueError:
                # If ISO format fails, try other common formats
                try:
                    # Try Unix timestamp (milliseconds)
                    timestamp_ms = float(data_copy["timestamp"])
                    data_copy["timestamp"] = datetime.fromtimestamp(timestamp_ms / 1000)
                except ValueError:
                    logger.warning(
                        f"Could not parse timestamp: {data_copy['timestamp']}"
                    )
                    # Fall back to current time
                    data_copy["timestamp"] = datetime.now()

        # Ensure all numeric fields are converted properly
        numeric_fields = ["open", "high", "low", "close", "volume", "trades"]
        for field in numeric_fields:
            if field in data_copy and data_copy[field] is not None:
                try:
                    data_copy[field] = float(data_copy[field])
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid {field} value: {data_copy[field]}, setting to 0"
                    )
                    data_copy[field] = 0.0

        # Extract metadata (any fields not in core_fields)
        metadata = {k: v for k, v in data_copy.items() if k not in core_fields}

        # Create candle instance with core fields
        kwargs = {k: data_copy[k] for k in core_fields if k in data_copy}
        kwargs["metadata"] = metadata

        return cls(**kwargs)

    def __str__(self) -> str:
        """String representation of the candle"""
        return (
            f"Candle({self.symbol}, {self.timestamp}, "
            f"O:{self.open:.2f}, H:{self.high:.2f}, L:{self.low:.2f}, C:{self.close:.2f}, "
            f"V:{self.volume:.2f})"
        )
