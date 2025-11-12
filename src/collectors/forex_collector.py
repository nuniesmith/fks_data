"""Forex tick data collector for EUR/USD and other currency pairs.

This module provides high-frequency tick data collection for forex pairs,
designed to support ASMBTR (Adaptive State Model on Binary Tree Representation)
which requires sub-second resolution price data.

Target: <1s resolution, >99% data completeness
Phase: AI Enhancement Plan Phase 1 - Data Preparation
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt  # type: ignore
from sqlalchemy import select  # type: ignore
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # type: ignore

logger = logging.getLogger(__name__)


class ForexTickCollector:
    """Collect high-frequency forex tick data via CCXT exchanges.

    Forex pairs are traded on crypto exchanges as stablecoins (e.g., EURUSD via EUR/USDT).
    This collector uses exchanges like Kraken, Binance that offer EUR pairs with
    tight spreads and high liquidity.

    Attributes:
        exchange_id: Exchange to use (default: kraken for EUR pairs)
        symbol: Trading pair symbol (default: EUR/USDT)
        resolution_ms: Target resolution in milliseconds (default: 1000 for 1s)
        max_gap_tolerance: Maximum acceptable gap in seconds (default: 5)
    """

    def __init__(
        self,
        exchange_id: str = "kraken",
        symbol: str = "EUR/USDT",
        resolution_ms: int = 1000,
        db_url: Optional[str] = None,
    ):
        """Initialize forex tick collector.

        Args:
            exchange_id: CCXT exchange identifier
            symbol: Trading pair (e.g., EUR/USDT, EUR/USD)
            resolution_ms: Target data resolution in milliseconds
            db_url: PostgreSQL connection URL (falls back to env var)
        """
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.resolution_ms = resolution_ms
        self.max_gap_tolerance = 5  # seconds

        # Exchange instance (created async)
        self.exchange: Optional[ccxt.Exchange] = None

        # Database
        self.db_url = db_url or self._get_db_url()
        self.engine = None

        # Metrics
        self.ticks_collected = 0
        self.data_gaps = 0
        self.last_tick_time: Optional[datetime] = None

    def _get_db_url(self) -> str:
        """Get database URL from environment."""
        import os
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        host = os.getenv("POSTGRES_HOST", "db")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "trading_db")

        return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    async def initialize(self) -> None:
        """Initialize exchange connection and database engine."""
        logger.info(f"Initializing ForexTickCollector: {self.exchange_id} - {self.symbol}")

        # Initialize exchange
        exchange_class = getattr(ccxt, self.exchange_id)
        self.exchange = exchange_class({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        # Load markets
        await self.exchange.load_markets()

        # Verify symbol exists
        if self.symbol not in self.exchange.markets:
            available = [s for s in self.exchange.markets if 'EUR' in s]
            raise ValueError(
                f"Symbol {self.symbol} not available on {self.exchange_id}. "
                f"Available EUR pairs: {available[:5]}"
            )

        # Initialize database engine
        self.engine = create_async_engine(self.db_url, echo=False)

        logger.info(f"✅ Initialized: {self.symbol} on {self.exchange_id}")

    async def fetch_ticker(self) -> dict[str, Any]:
        """Fetch current ticker (bid/ask/last) for the symbol.

        Returns:
            Ticker data with timestamp, bid, ask, last price, volume
        """
        if not self.exchange:
            raise RuntimeError("Exchange not initialized. Call initialize() first.")

        ticker = await self.exchange.fetch_ticker(self.symbol)

        return {
            'timestamp': datetime.fromtimestamp(ticker['timestamp'] / 1000),
            'symbol': self.symbol,
            'bid': Decimal(str(ticker.get('bid', 0))),
            'ask': Decimal(str(ticker.get('ask', 0))),
            'last': Decimal(str(ticker.get('last', 0))),
            'volume': Decimal(str(ticker.get('baseVolume', 0))),
            'exchange': self.exchange_id
        }

    async def fetch_orderbook_snapshot(self, limit: int = 5) -> dict[str, Any]:
        """Fetch orderbook snapshot for spread analysis.

        Args:
            limit: Number of bid/ask levels to fetch

        Returns:
            Orderbook with bids, asks, timestamp
        """
        if not self.exchange:
            raise RuntimeError("Exchange not initialized.")

        orderbook = await self.exchange.fetch_order_book(self.symbol, limit=limit)

        return {
            'timestamp': datetime.fromtimestamp(orderbook['timestamp'] / 1000),
            'symbol': self.symbol,
            'bids': orderbook['bids'][:limit],  # [[price, size], ...]
            'asks': orderbook['asks'][:limit],
            'spread': orderbook['asks'][0][0] - orderbook['bids'][0][0] if orderbook['bids'] and orderbook['asks'] else 0
        }

    async def stream_ticks(self, duration_seconds: int = 60) -> list[dict[str, Any]]:
        """Stream tick data for specified duration.

        Args:
            duration_seconds: How long to collect data

        Returns:
            List of tick dictionaries
        """
        if not self.exchange:
            await self.initialize()

        ticks = []
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration_seconds)

        logger.info(f"📊 Streaming {self.symbol} ticks for {duration_seconds}s...")

        while datetime.now() < end_time:
            try:
                tick = await self.fetch_ticker()
                ticks.append(tick)

                # Track metrics
                self.ticks_collected += 1
                if self.last_tick_time:
                    gap = (tick['timestamp'] - self.last_tick_time).total_seconds()
                    if gap > self.max_gap_tolerance:
                        self.data_gaps += 1
                        logger.warning(f"⚠️ Data gap detected: {gap:.2f}s")

                self.last_tick_time = tick['timestamp']

                # Wait for next tick (target resolution)
                await asyncio.sleep(self.resolution_ms / 1000)

            except Exception as e:
                logger.error(f"❌ Error fetching tick: {e}")
                await asyncio.sleep(1)

        completeness = 100 * (1 - self.data_gaps / max(self.ticks_collected, 1))
        logger.info(
            f"✅ Collected {len(ticks)} ticks, "
            f"{self.data_gaps} gaps, "
            f"completeness: {completeness:.2f}%"
        )

        return ticks

    async def save_ticks_to_db(self, ticks: list[dict[str, Any]]) -> int:
        """Save collected ticks to TimescaleDB.

        Args:
            ticks: List of tick dictionaries

        Returns:
            Number of ticks saved
        """
        if not self.engine:
            raise RuntimeError("Database engine not initialized.")

        # TODO: Implement TimescaleDB insertion
        # This will be implemented after we verify hypertable structure in Phase 1 Task 3
        logger.info(f"💾 Would save {len(ticks)} ticks to database (not yet implemented)")

        return len(ticks)

    async def get_data_quality_metrics(self) -> dict[str, Any]:
        """Calculate data quality metrics.

        Returns:
            Dictionary with completeness, gap count, tick count
        """
        completeness = 100 * (1 - self.data_gaps / max(self.ticks_collected, 1))

        return {
            'ticks_collected': self.ticks_collected,
            'data_gaps': self.data_gaps,
            'completeness_pct': round(completeness, 2),
            'resolution_ms': self.resolution_ms,
            'symbol': self.symbol,
            'exchange': self.exchange_id,
            'last_tick': self.last_tick_time.isoformat() if self.last_tick_time else None
        }

    async def cleanup(self) -> None:
        """Close exchange connection and database engine."""
        if self.exchange:
            await self.exchange.close()
            logger.info(f"Closed {self.exchange_id} connection")

        if self.engine:
            await self.engine.dispose()
            logger.info("Closed database engine")


async def main():
    """Example usage: Collect EUR/USD ticks for 60 seconds."""
    collector = ForexTickCollector(
        exchange_id="kraken",
        symbol="EUR/USDT",
        resolution_ms=1000  # 1 second ticks
    )

    try:
        await collector.initialize()

        # Stream ticks for 60 seconds
        ticks = await collector.stream_ticks(duration_seconds=60)

        # Get quality metrics
        metrics = await collector.get_data_quality_metrics()
        print("\n📊 Data Quality Metrics:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")

        # Display sample ticks
        if ticks:
            print(f"\n📈 Sample Ticks (first 3 of {len(ticks)}):")
            for tick in ticks[:3]:
                print(f"  {tick['timestamp']}: bid={tick['bid']}, ask={tick['ask']}, last={tick['last']}")

    finally:
        await collector.cleanup()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())
