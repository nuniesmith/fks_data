"""
Tick data model representing individual trades/transactions.

This module provides a standardized representation of tick data
across all data sources and exchanges in the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Literal, Optional, Union

# Try to use our centralized logging, fall back to loguru if not available
try:
    from app.trading.logging import get_logger

    logger = get_logger("data.models.tick")
except ImportError:
    try:
        from loguru import logger
    except ImportError:
        import logging

        logger = logging.getLogger("data.models.tick")
        logger.setLevel(logging.INFO)

# Import base Model class, fall back to a simple implementation if not available
try:
    from core.models.base import Model

    BaseClass = Model
except ImportError:
    logger.debug("core.model.Model not available, using basic object as base class")

    class BaseClass:
        """Simple base class fallback"""

        pass


@dataclass
class Tick(BaseClass):
    """
    Represents a single trade or transaction in the market.

    Attributes:
        symbol: The trading pair or ticker symbol (e.g., 'BTC/USD')
        timestamp: The exact time the trade occurred
        price: The price at which the trade executed
        volume: The volume traded
        side: The side of the trade (buy/sell)
        trade_id: Unique identifier for the trade (if available)
        source: Data source that provided this tick data
        buyer_order_id: ID of the buyer's order (if available)
        seller_order_id: ID of the seller's order (if available)
        is_market_maker: Whether the maker of the trade was a market maker
        metadata: Additional data source specific information
    """

    symbol: str
    timestamp: datetime
    price: float
    volume: float
    side: Optional[Literal["buy", "sell"]] = None
    trade_id: Optional[str] = None
    source: str = "unknown"
    buyer_order_id: Optional[str] = None
    seller_order_id: Optional[str] = None
    is_market_maker: Optional[bool] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate tick data after initialization"""
        # Ensure price is non-negative
        self.price = max(0, self.price)

        # Ensure volume is non-negative
        self.volume = max(0, self.volume)

        # Normalize side to lowercase if present
        if self.side:
            self.side = self.side.lower()

    @property
    def value(self) -> float:
        """
        Calculate the total value of this trade.

        Returns:
            The price multiplied by the volume
        """
        return self.price * self.volume

    @property
    def is_buy(self) -> bool:
        """
        Check if this is a buy trade.

        Returns:
            True if the side is 'buy', False otherwise
        """
        return self.side == "buy"

    @property
    def is_sell(self) -> bool:
        """
        Check if this is a sell trade.

        Returns:
            True if the side is 'sell', False otherwise
        """
        return self.side == "sell"

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the tick to a dictionary.

        Returns:
            Dictionary representation of the tick
        """
        result = {
            "symbol": self.symbol,
            "timestamp": (
                self.timestamp.isoformat()
                if isinstance(self.timestamp, datetime)
                else self.timestamp
            ),
            "price": self.price,
            "volume": self.volume,
            "source": self.source,
        }

        # Add optional fields if they're not None
        optional_fields = [
            "side",
            "trade_id",
            "buyer_order_id",
            "seller_order_id",
            "is_market_maker",
        ]

        for field in optional_fields:
            value = getattr(self, field)
            if value is not None:
                result[field] = value

        # Add metadata
        result.update(self.metadata)

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tick":
        """
        Create a Tick instance from a dictionary.

        Args:
            data: Dictionary containing tick data

        Returns:
            A new Tick instance
        """
        # Make a copy to avoid modifying the original
        data_copy = data.copy()

        # Extract the core fields
        core_fields = [
            "symbol",
            "timestamp",
            "price",
            "volume",
            "side",
            "trade_id",
            "source",
            "buyer_order_id",
            "seller_order_id",
            "is_market_maker",
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
        for field in ["price", "volume"]:
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

        # Create tick instance with core fields
        kwargs = {k: data_copy[k] for k in core_fields if k in data_copy}
        kwargs["metadata"] = metadata

        return cls(**kwargs)

    def __str__(self) -> str:
        """String representation of the tick"""
        side_str = f"({self.side})" if self.side else ""
        return (
            f"Tick({self.symbol}, {self.timestamp}, "
            f"P:{self.price:.2f}, V:{self.volume:.2f} {side_str})"
        )
