"""
Market data model representing current market state and statistics.

This module provides standardized representations of market data
and order books across all data sources and exchanges in the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

# Try to use our centralized logging, fall back to loguru if not available
try:
    from app.trading.logging import get_logger

    logger = get_logger("data.models.market")
except ImportError:
    try:
        from loguru import logger
    except ImportError:
        import logging

        logger = logging.getLogger("data.models.market")
        logger.setLevel(logging.INFO)

# Import the actual Model class - no more fallback mechanism
from core.models.base import Model

T = TypeVar("T", bound="OrderBookEntry")


@dataclass
class MarketData(Model):
    """
    Represents comprehensive market data for a trading pair or asset.

    Attributes:
        symbol: The trading pair or ticker symbol (e.g., 'BTC/USD')
        timestamp: The timestamp when this data was captured
        bid: Current highest bid price
        ask: Current lowest ask price
        last: Last traded price (also called 'last_price' in some systems)
        volume_24h: Trading volume in the last 24 hours
        high_24h: Highest price in the last 24 hours
        low_24h: Lowest price in the last 24 hours
        price_change_24h: Price change in the last 24 hours (absolute)
        price_change_pct_24h: Price change percentage in the last 24 hours
        base_volume: Volume in base currency
        quote_volume: Volume in quote currency
        bid_volume: Volume available at the best bid
        ask_volume: Volume available at the best ask
        order_book_depth: Current order book depth metrics
        exchange: Exchange or venue that provided this data
        source: Data source that provided this market data
        metadata: Additional data source specific information
    """

    # Define schema using Model's class variables
    _fields = {
        "symbol": str,
        "timestamp": datetime,
        "bid": float,
        "ask": float,
        "last": float,
        "volume_24h": float,
        "high_24h": float,
        "low_24h": float,
        "price_change_24h": float,
        "price_change_pct_24h": float,
        "base_volume": Optional[float],
        "quote_volume": Optional[float],
        "bid_volume": Optional[float],
        "ask_volume": Optional[float],
        "order_book_depth": Optional[Dict[str, Any]],
        "exchange": str,
        "source": str,
        "metadata": Dict[str, Any],
    }

    _required_fields = [
        "symbol",
        "timestamp",
        "bid",
        "ask",
        "last",
        "volume_24h",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_pct_24h",
    ]

    symbol: str
    timestamp: datetime
    bid: float
    ask: float
    last: float
    volume_24h: float
    high_24h: float
    low_24h: float
    price_change_24h: float
    price_change_pct_24h: float
    base_volume: Optional[float] = None
    quote_volume: Optional[float] = None
    bid_volume: Optional[float] = None
    ask_volume: Optional[float] = None
    order_book_depth: Optional[Dict[str, Any]] = field(default_factory=dict)
    exchange: str = "unknown"
    source: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Call validate to ensure all data is valid"""
        self.validate()

    def _validate(self) -> None:
        """Custom validation using Model's validation framework"""
        # Ensure prices are non-negative
        if self.bid < 0:
            self.bid = 0
        if self.ask < 0:
            self.ask = 0
        if self.last < 0:
            self.last = 0

        # Ensure volumes are non-negative
        if self.volume_24h < 0:
            self.volume_24h = 0
        if self.base_volume is not None and self.base_volume < 0:
            self.base_volume = 0
        if self.quote_volume is not None and self.quote_volume < 0:
            self.quote_volume = 0
        if self.bid_volume is not None and self.bid_volume < 0:
            self.bid_volume = 0
        if self.ask_volume is not None and self.ask_volume < 0:
            self.ask_volume = 0

    @property
    def spread(self) -> float:
        """
        Calculate the current bid-ask spread.

        Returns:
            The absolute difference between ask and bid
        """
        return self.ask - self.bid

    @property
    def spread_pct(self) -> float:
        """
        Calculate the current bid-ask spread as a percentage.

        Returns:
            The spread as a percentage of the bid price
        """
        if self.bid == 0:
            return 0
        return (self.spread / self.bid) * 100

    @property
    def spread_percentage(self) -> float:
        """Alias for spread_pct for backward compatibility"""
        return self.spread_pct

    @property
    def mid_price(self) -> float:
        """
        Calculate the mid price between bid and ask.

        Returns:
            The average of bid and ask prices
        """
        return (self.bid + self.ask) / 2

    @property
    def last_price(self) -> float:
        """Alias for 'last' for backward compatibility"""
        return self.last

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the market data to a dictionary.

        Returns:
            Dictionary representation of the market data
        """
        # Start with the base Model's to_dict
        result = super().to_dict()

        # Add computed properties
        result.update(
            {
                "spread": self.spread,
                "spread_pct": self.spread_pct,
                "mid_price": self.mid_price,
            }
        )

        # Format timestamp
        if isinstance(result["timestamp"], datetime):
            result["timestamp"] = result["timestamp"].isoformat()

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MarketData":
        """
        Create a MarketData instance from a dictionary.

        Args:
            data: Dictionary containing market data

        Returns:
            A new MarketData instance
        """
        # Make a copy to avoid modifying the original
        data_copy = data.copy()

        # Process timestamp if it's a string
        if "timestamp" in data_copy and isinstance(data_copy["timestamp"], str):
            try:
                data_copy["timestamp"] = datetime.fromisoformat(data_copy["timestamp"])
            except ValueError:
                # If ISO format fails, try other common formats
                try:
                    timestamp_ms = float(data_copy["timestamp"])
                    data_copy["timestamp"] = datetime.fromtimestamp(timestamp_ms / 1000)
                except ValueError:
                    logger.warning(
                        f"Could not parse timestamp: {data_copy['timestamp']}"
                    )
                    data_copy["timestamp"] = datetime.now()

        # Define required fields with default values
        required_fields = {
            "symbol": "",
            "timestamp": datetime.now(),
            "bid": 0.0,
            "ask": 0.0,
            "last": 0.0,
            "volume_24h": 0.0,
            "high_24h": 0.0,
            "low_24h": 0.0,
            "price_change_24h": 0.0,
            "price_change_pct_24h": 0.0,
        }

        # Define optional fields with None as default
        optional_fields = [
            "base_volume",
            "quote_volume",
            "bid_volume",
            "ask_volume",
            "order_book_depth",
            "exchange",
            "source",
        ]

        # Ensure required fields exist and are the correct types
        kwargs = {}
        for field_name, default_value in required_fields.items():
            try:
                if field_name in data_copy:
                    if (
                        isinstance(default_value, (int, float))
                        and data_copy[field_name] is not None
                    ):
                        kwargs[field_name] = float(data_copy[field_name])
                    else:
                        kwargs[field_name] = data_copy[field_name]
                else:
                    kwargs[field_name] = default_value
                    logger.warning(
                        f"Missing required field '{field_name}', using default: {default_value}"
                    )
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Error processing field '{field_name}': {e}, using default: {default_value}"
                )
                kwargs[field_name] = default_value

        # Add optional fields if they exist
        for field in optional_fields:
            if field in data_copy:
                try:
                    if (
                        field
                        in ["base_volume", "quote_volume", "bid_volume", "ask_volume"]
                        and data_copy[field] is not None
                    ):
                        kwargs[field] = float(data_copy[field])
                    else:
                        kwargs[field] = data_copy[field]
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Error processing optional field '{field}': {e}, skipping"
                    )

        # Handle last_price -> last field conversion for compatibility
        if "last_price" in data_copy and "last" not in data_copy:
            try:
                kwargs["last"] = float(data_copy["last_price"])
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting last_price to last: {e}")

        # Extract metadata (fields not in required or optional fields)
        all_fields = list(required_fields.keys()) + optional_fields
        metadata = {k: v for k, v in data_copy.items() if k not in all_fields}
        kwargs["metadata"] = metadata

        return cls(**kwargs)

    def __str__(self) -> str:
        """String representation of the market data"""
        return (
            f"MarketData({self.symbol}, {self.timestamp}, "
            f"Bid:{self.bid:.2f}, Ask:{self.ask:.2f}, Last:{self.last:.2f}, "
            f"Vol:{self.volume_24h:.2f})"
        )


@dataclass
class OrderBookEntry(Model):
    """
    Represents a single entry in an order book.

    Attributes:
        price: The price level
        volume: The volume available at this price
        count: The number of orders at this price level (if available)
    """

    # Define schema using Model's class variables
    _fields = {"price": float, "volume": float, "count": Optional[int]}

    _required_fields = ["price", "volume"]

    price: float
    volume: float
    count: Optional[int] = None

    def __post_init__(self):
        """Call validate to ensure all data is valid"""
        self.validate()

    def _validate(self) -> None:
        """Validate order book entry data"""
        if self.price < 0:
            self.price = 0
        if self.volume < 0:
            self.volume = 0
        if self.count is not None and self.count < 0:
            self.count = 0

    @classmethod
    def from_tuple(cls: "type[T]", data: Tuple[float, float, Optional[int]]) -> T:
        """Create an OrderBookEntry from a tuple representation"""
        price = float(data[0])
        volume = float(data[1])
        count = int(data[2]) if len(data) > 2 and data[2] is not None else None
        return cls(price=price, volume=volume, count=count)


@dataclass
class OrderBook(Model):
    """
    Represents a snapshot of a market's order book.

    Attributes:
        symbol: The trading pair or ticker symbol
        timestamp: The timestamp when this snapshot was taken
        bids: List of order book entries for bids (sorted by price descending)
        asks: List of order book entries for asks (sorted by price ascending)
        source: Data source that provided this order book
        exchange: Exchange or venue that provided this data
        depth: The depth of the order book (number of price levels)
    """

    # Define schema using Model's class variables
    _fields = {
        "symbol": str,
        "timestamp": datetime,
        "bids": List[OrderBookEntry],
        "asks": List[OrderBookEntry],
        "source": str,
        "exchange": str,
    }

    _required_fields = ["symbol", "timestamp", "bids", "asks"]

    symbol: str
    timestamp: datetime
    bids: List[Union[OrderBookEntry, Tuple[float, float, Optional[int]]]]
    asks: List[Union[OrderBookEntry, Tuple[float, float, Optional[int]]]]
    source: str = "unknown"
    exchange: str = "unknown"
    depth: int = field(init=False)

    def __post_init__(self):
        """Process and validate order book after initialization"""
        # Convert tuple bids/asks to OrderBookEntry if necessary
        self._convert_entries()

        # Sort bids and asks appropriately
        self._sort_entries()

        # Calculate depth
        self.depth = max(len(self.bids), len(self.asks))

        # Call validate method from Model
        self.validate()

    def _validate(self) -> None:
        """Custom validation for OrderBook"""
        # Most validation is handled in __post_init__
        pass

    def _convert_entries(self):
        """Convert tuple entries to OrderBookEntry objects if needed"""
        # Process bids
        processed_bids: List[OrderBookEntry] = []
        for entry in self.bids:
            if isinstance(entry, OrderBookEntry):
                processed_bids.append(entry)
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                processed_bids.append(
                    OrderBookEntry(
                        price=float(entry[0]),
                        volume=float(entry[1]),
                        count=(
                            int(entry[2])
                            if len(entry) > 2 and entry[2] is not None
                            else None
                        ),
                    )
                )
            else:
                logger.warning(f"Invalid bid entry type: {type(entry)}, skipping")
        self.bids = processed_bids

        # Process asks
        processed_asks: List[OrderBookEntry] = []
        for entry in self.asks:
            if isinstance(entry, OrderBookEntry):
                processed_asks.append(entry)
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                processed_asks.append(
                    OrderBookEntry(
                        price=float(entry[0]),
                        volume=float(entry[1]),
                        count=(
                            int(entry[2])
                            if len(entry) > 2 and entry[2] is not None
                            else None
                        ),
                    )
                )
            else:
                logger.warning(f"Invalid ask entry type: {type(entry)}, skipping")
        self.asks = processed_asks

    def _sort_entries(self):
        """Sort bids descending and asks ascending by price"""
        self.bids = sorted(self.bids, key=lambda x: x.price, reverse=True)
        self.asks = sorted(self.asks, key=lambda x: x.price)

    @property
    def spread(self) -> float:
        """
        Calculate the current bid-ask spread.

        Returns:
            The spread between best bid and best ask, or 0 if order book is empty
        """
        if not self.bids or not self.asks:
            return 0
        return self.asks[0].price - self.bids[0].price

    @property
    def mid_price(self) -> float:
        """
        Calculate the mid price.

        Returns:
            The average of best bid and best ask, or 0 if order book is empty
        """
        if not self.bids or not self.asks:
            return 0
        return (self.asks[0].price + self.bids[0].price) / 2

    @property
    def best_bid(self) -> Optional[OrderBookEntry]:
        """Get the best (highest) bid"""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[OrderBookEntry]:
        """Get the best (lowest) ask"""
        return self.asks[0] if self.asks else None

    def bid_volume_at_price(self, price: float) -> float:
        """
        Get the total bid volume at or above a specified price.

        Args:
            price: The price threshold

        Returns:
            The total volume
        """
        return sum(entry.volume for entry in self.bids if entry.price >= price)

    def ask_volume_at_price(self, price: float) -> float:
        """
        Get the total ask volume at or below a specified price.

        Args:
            price: The price threshold

        Returns:
            The total volume
        """
        return sum(entry.volume for entry in self.asks if entry.price <= price)

    def get_volume_imbalance(self, levels: int = 5) -> float:
        """
        Calculate volume imbalance between bid and ask sides.

        Returns a value between -1 and 1 where:
        - Positive values indicate more volume on the bid side (bullish)
        - Negative values indicate more volume on the ask side (bearish)
        - Zero indicates balance

        Args:
            levels: Number of price levels to use

        Returns:
            Volume imbalance ratio
        """
        bid_vol = sum(entry.volume for entry in self.bids[:levels]) if self.bids else 0
        ask_vol = sum(entry.volume for entry in self.asks[:levels]) if self.asks else 0

        total_vol = bid_vol + ask_vol
        if total_vol == 0:
            return 0

        return (bid_vol - ask_vol) / total_vol

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the order book to a dictionary.

        Returns:
            Dictionary representation of the order book
        """
        # Start with the base Model's to_dict
        result = super().to_dict()

        # Add formatted timestamp
        result["timestamp"] = (
            self.timestamp.isoformat()
            if isinstance(self.timestamp, datetime)
            else self.timestamp
        )

        # Add the bid and ask entries
        result["bids"] = [bid.to_dict() for bid in self.bids]
        result["asks"] = [ask.to_dict() for ask in self.asks]

        # Add computed properties
        result.update(
            {"depth": self.depth, "spread": self.spread, "mid_price": self.mid_price}
        )

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderBook":
        """
        Create an OrderBook instance from a dictionary.

        Args:
            data: Dictionary containing order book data

        Returns:
            A new OrderBook instance
        """
        # Make a copy to avoid modifying the original
        data_copy = data.copy()

        # Process timestamp if it's a string
        if "timestamp" in data_copy and isinstance(data_copy["timestamp"], str):
            try:
                data_copy["timestamp"] = datetime.fromisoformat(data_copy["timestamp"])
            except ValueError:
                logger.warning(f"Could not parse timestamp: {data_copy['timestamp']}")
                data_copy["timestamp"] = datetime.now()

        # Process bids and asks
        bids: List[OrderBookEntry] = []
        asks: List[OrderBookEntry] = []

        # Handle different formats of bids/asks
        if "bids" in data_copy:
            for bid in data_copy["bids"]:
                if isinstance(bid, dict):
                    bids.append(OrderBookEntry(**bid))
                elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    bids.append(
                        OrderBookEntry(
                            price=float(bid[0]),
                            volume=float(bid[1]),
                            count=(
                                int(bid[2])
                                if len(bid) > 2 and bid[2] is not None
                                else None
                            ),
                        )
                    )
                else:
                    logger.warning(f"Invalid bid format: {bid}, skipping")

        if "asks" in data_copy:
            for ask in data_copy["asks"]:
                if isinstance(ask, dict):
                    asks.append(OrderBookEntry(**ask))
                elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    asks.append(
                        OrderBookEntry(
                            price=float(ask[0]),
                            volume=float(ask[1]),
                            count=(
                                int(ask[2])
                                if len(ask) > 2 and ask[2] is not None
                                else None
                            ),
                        )
                    )
                else:
                    logger.warning(f"Invalid ask format: {ask}, skipping")

        # Create OrderBook instance
        order_book = cls(
            symbol=data_copy.get("symbol", ""),
            timestamp=data_copy.get("timestamp", datetime.now()),
            bids=bids,
            asks=asks,
            source=data_copy.get("source", "unknown"),
            exchange=data_copy.get("exchange", "unknown"),
        )

        return order_book

    def __str__(self) -> str:
        """String representation of the order book"""
        best_bid = f"@{self.bids[0].price:.2f}" if self.bids else "None"
        best_ask = f"@{self.asks[0].price:.2f}" if self.asks else "None"
        return (
            f"OrderBook({self.symbol}, {self.timestamp}, "
            f"Levels: {self.depth}, Best bid: {best_bid}, Best ask: {best_ask})"
        )
