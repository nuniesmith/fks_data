"""Delta (Δ) scanner for micro-price changes.

This module detects and analyzes micro-price changes (<0.01%) from tick data,
which serves as the foundation for BTR (Binary Tree Representation) encoding
in the ASMBTR strategy.

The scanner identifies price movements at the tick level and categorizes them
for state-based analysis.

Phase: AI Enhancement Plan Phase 1 - Data Preparation
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PriceDirection(Enum):
    """Price movement direction."""
    UP = 1
    DOWN = -1
    NEUTRAL = 0


@dataclass
class PriceChange:
    """Represents a detected price change.

    Attributes:
        timestamp: When the change occurred
        symbol: Trading pair
        old_price: Previous price
        new_price: Current price
        delta: Absolute price change
        delta_pct: Percentage change
        direction: Price direction (UP/DOWN/NEUTRAL)
        is_micro: Whether this is a micro-change (<0.01%)
    """
    timestamp: datetime
    symbol: str
    old_price: Decimal
    new_price: Decimal
    delta: Decimal
    delta_pct: float
    direction: PriceDirection
    is_micro: bool

    def to_binary(self) -> str:
        """Convert to binary representation for BTR encoding.

        Returns:
            "1" for up, "0" for down, "" for neutral
        """
        if self.direction == PriceDirection.UP:
            return "1"
        elif self.direction == PriceDirection.DOWN:
            return "0"
        return ""


class DeltaScanner:
    """Scanner for detecting micro-price changes in tick data.

    This scanner is optimized for high-frequency forex data where micro-changes
    (<0.01%) are significant for state-based trading models.

    Attributes:
        micro_threshold: Threshold for micro-changes (default: 0.01%)
        min_change: Minimum absolute change to consider (default: 0.00001 for forex)
    """

    def __init__(
        self,
        micro_threshold: float = 0.01,  # 0.01% = 1 basis point
        min_change: float = 0.00001,  # 0.001 cents for EUR/USD
    ):
        """Initialize delta scanner.

        Args:
            micro_threshold: Percentage threshold for micro-changes
            min_change: Minimum absolute price change to detect
        """
        self.micro_threshold = micro_threshold
        self.min_change = Decimal(str(min_change))

        # Tracking
        self.last_price: Optional[Decimal] = None
        self.changes: list[PriceChange] = []

        # Statistics
        self.total_changes = 0
        self.micro_changes = 0
        self.up_changes = 0
        self.down_changes = 0
        self.neutral_changes = 0

    def scan_tick(
        self,
        timestamp: datetime,
        symbol: str,
        price: Decimal
    ) -> Optional[PriceChange]:
        """Scan a single tick for price changes.

        Args:
            timestamp: Tick timestamp
            symbol: Trading pair
            price: Current price

        Returns:
            PriceChange if change detected, None otherwise
        """
        if self.last_price is None:
            self.last_price = price
            return None

        # Calculate delta
        delta = price - self.last_price

        # Check if change is significant enough
        if abs(delta) < self.min_change:
            self.neutral_changes += 1
            direction = PriceDirection.NEUTRAL
        else:
            direction = PriceDirection.UP if delta > 0 else PriceDirection.DOWN

        # Calculate percentage change
        delta_pct = float((delta / self.last_price) * 100) if self.last_price != 0 else 0.0

        # Determine if micro-change
        is_micro = abs(delta_pct) < self.micro_threshold

        # Create change object
        change = PriceChange(
            timestamp=timestamp,
            symbol=symbol,
            old_price=self.last_price,
            new_price=price,
            delta=delta,
            delta_pct=delta_pct,
            direction=direction,
            is_micro=is_micro
        )

        # Update tracking
        self.last_price = price
        self.changes.append(change)
        self.total_changes += 1

        if is_micro:
            self.micro_changes += 1

        if direction == PriceDirection.UP:
            self.up_changes += 1
        elif direction == PriceDirection.DOWN:
            self.down_changes += 1

        return change

    def scan_ticks(
        self,
        ticks: list[dict],
        price_key: str = 'last'
    ) -> list[PriceChange]:
        """Scan multiple ticks for price changes.

        Args:
            ticks: List of tick dictionaries
            price_key: Key to extract price from tick dict (default: 'last')

        Returns:
            List of detected price changes
        """
        changes = []

        for tick in ticks:
            timestamp = tick.get('timestamp')
            symbol = tick.get('symbol', 'UNKNOWN')
            price = tick.get(price_key)

            if not isinstance(price, Decimal):
                price = Decimal(str(price))

            change = self.scan_tick(timestamp, symbol, price)
            if change:
                changes.append(change)

        return changes

    def get_binary_sequence(self, max_length: Optional[int] = None) -> str:
        """Get binary sequence of price changes for BTR encoding.

        Args:
            max_length: Maximum sequence length (most recent)

        Returns:
            Binary string of price movements (e.g., "10110011")
        """
        changes = self.changes[-max_length:] if max_length else self.changes

        # Filter out neutral changes
        significant_changes = [c for c in changes if c.direction != PriceDirection.NEUTRAL]

        return ''.join(c.to_binary() for c in significant_changes)

    def get_statistics(self) -> dict[str, any]:
        """Calculate scanner statistics.

        Returns:
            Dictionary with change counts, ratios, micro-change percentage
        """
        micro_pct = (self.micro_changes / self.total_changes * 100) if self.total_changes > 0 else 0
        up_pct = (self.up_changes / self.total_changes * 100) if self.total_changes > 0 else 0
        down_pct = (self.down_changes / self.total_changes * 100) if self.total_changes > 0 else 0
        neutral_pct = (self.neutral_changes / self.total_changes * 100) if self.total_changes > 0 else 0

        return {
            'total_changes': self.total_changes,
            'micro_changes': self.micro_changes,
            'micro_pct': round(micro_pct, 2),
            'up_changes': self.up_changes,
            'up_pct': round(up_pct, 2),
            'down_changes': self.down_changes,
            'down_pct': round(down_pct, 2),
            'neutral_changes': self.neutral_changes,
            'neutral_pct': round(neutral_pct, 2),
            'micro_threshold': self.micro_threshold,
            'min_change': float(self.min_change)
        }

    def get_recent_changes(self, count: int = 10) -> list[PriceChange]:
        """Get most recent price changes.

        Args:
            count: Number of recent changes to return

        Returns:
            List of PriceChange objects
        """
        return self.changes[-count:] if self.changes else []

    def reset(self) -> None:
        """Reset scanner state."""
        self.last_price = None
        self.changes.clear()
        self.total_changes = 0
        self.micro_changes = 0
        self.up_changes = 0
        self.down_changes = 0
        self.neutral_changes = 0
        logger.info("Scanner reset")


def analyze_tick_data(ticks: list[dict], price_key: str = 'last') -> dict:
    """Convenience function to analyze tick data.

    Args:
        ticks: List of tick dictionaries
        price_key: Key to extract price from tick dict

    Returns:
        Dictionary with scanner statistics and binary sequence
    """
    scanner = DeltaScanner()
    changes = scanner.scan_ticks(ticks, price_key=price_key)

    stats = scanner.get_statistics()
    binary_seq = scanner.get_binary_sequence(max_length=64)  # Last 64 moves

    return {
        **stats,
        'binary_sequence': binary_seq,
        'sequence_length': len(binary_seq),
        'changes': changes
    }


if __name__ == "__main__":
    """Example usage with simulated tick data."""
    from datetime import datetime, timedelta
    from decimal import Decimal

    # Simulate EUR/USD tick data with micro-changes
    base_price = Decimal("1.08500")
    ticks = []

    for i in range(100):
        # Simulate random micro-changes
        import random
        change_pct = random.uniform(-0.015, 0.015)  # ±0.015% (~1.5 basis points)
        new_price = base_price * (1 + Decimal(str(change_pct / 100)))

        ticks.append({
            'timestamp': datetime.now() + timedelta(seconds=i),
            'symbol': 'EUR/USD',
            'last': new_price
        })

        base_price = new_price

    # Analyze
    logging.basicConfig(level=logging.INFO)
    results = analyze_tick_data(ticks)

    print("\n📊 Delta Scanner Results:")
    print(f"  Total changes: {results['total_changes']}")
    print(f"  Micro-changes (<0.01%): {results['micro_changes']} ({results['micro_pct']}%)")
    print(f"  Up: {results['up_changes']} ({results['up_pct']}%)")
    print(f"  Down: {results['down_changes']} ({results['down_pct']}%)")
    print(f"  Neutral: {results['neutral_changes']} ({results['neutral_pct']}%)")
    print(f"\n🔢 Binary sequence (last 64): {results['binary_sequence']}")
    print(f"  Sequence length: {results['sequence_length']}")
