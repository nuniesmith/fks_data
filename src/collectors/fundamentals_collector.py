"""Fundamentals data collector using EODHD API.

This collector fetches company fundamental data, earnings, and economic indicators
from EODHD API to support AI-driven trading strategies that incorporate
fundamental analysis alongside technical indicators.

Target: Daily updates for fundamentals, real-time earnings calendar
Phase: AI Enhancement Plan Phase 1 - Data Foundation
"""

import asyncio
import logging
import os
import time
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from adapters import get_adapter  # type: ignore
except ImportError:
    # Fallback imports if shared_python not available
    class DataFetchError(Exception):
        def __init__(self, provider: str, message: str):
            self.provider = provider
            super().__init__(f"{provider}: {message}")

logger = logging.getLogger(__name__)


class FundamentalsCollector:
    """Collect fundamental data from EODHD API.

    This collector fetches:
    - Company financials (balance sheet, income statement, cash flow)
    - Earnings data and estimates
    - Economic indicators (GDP, inflation, interest rates)
    - Insider transactions

    Attributes:
        symbols: List of symbols to collect data for
        collection_interval: Hours between full collection cycles
        earnings_symbols: Symbols to monitor for earnings
        economic_countries: Countries for economic indicators
    """

    def __init__(
        self,
        symbols: Optional[list[str]] = None,
        collection_interval: int = 24,  # hours
        earnings_symbols: Optional[list[str]] = None,
        economic_countries: Optional[list[str]] = None,
    ):
        """Initialize fundamentals collector.

        Args:
            symbols: List of symbols to collect (default: major stocks)
            collection_interval: Hours between collection cycles
            earnings_symbols: Symbols for earnings monitoring
            economic_countries: Countries for economic data (US, EU, etc.)
        """
        # Default symbols: Major stocks + some crypto-friendly companies
        self.symbols = symbols or [
            "AAPL.US", "MSFT.US", "GOOGL.US", "AMZN.US", "TSLA.US",
            "NVDA.US", "META.US", "BRK-B.US", "JNJ.US", "V.US",
            # Financial sector (crypto-relevant)
            "JPM.US", "BAC.US", "GS.US", "MS.US",
            # Tech/crypto related
            "PYPL.US", "SQ.US", "COIN.US", "MSTR.US"
        ]

        self.collection_interval = collection_interval
        self.earnings_symbols = earnings_symbols or self.symbols[:10]  # Top 10 for earnings
        self.economic_countries = economic_countries or ["US", "EU", "CN", "JP"]

        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.0  # 1 second between requests

        # Initialize EODHD adapter
        try:
            self.adapter = get_adapter("eodhd")
        except Exception as e:
            logger.error(f"Failed to initialize EODHD adapter: {e}")
            self.adapter = None

        logger.info(f"Initialized FundamentalsCollector with {len(self.symbols)} symbols")

    async def collect_fundamentals(self, symbol: str) -> Optional[dict[str, Any]]:
        """Collect fundamental data for a single symbol.

        Args:
            symbol: Symbol to collect (e.g., AAPL.US)

        Returns:
            Normalized fundamental data or None if failed
        """
        if not self.adapter:
            logger.error("EODHD adapter not available")
            return None

        try:
            # Rate limiting
            await self._rate_limit()

            logger.debug(f"Fetching fundamentals for {symbol}")
            result = self.adapter.fetch(
                data_type="fundamentals",
                symbol=symbol
            )

            if result and result.get("data"):
                logger.info(f"✅ Collected fundamentals for {symbol}")
                return result["data"][0]  # Get first (and only) record
            else:
                logger.warning(f"⚠️ No fundamental data returned for {symbol}")
                return None

        except DataFetchError as e:
            logger.error(f"❌ Data fetch error for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error collecting {symbol}: {e}")
            return None

    async def collect_earnings_calendar(
        self,
        symbols: Optional[list[str]] = None,
        days_ahead: int = 30
    ) -> list[dict[str, Any]]:
        """Collect earnings calendar data.

        Args:
            symbols: Symbols to monitor (default: self.earnings_symbols)
            days_ahead: Days ahead to look for earnings

        Returns:
            List of earnings events
        """
        if not self.adapter:
            logger.error("EODHD adapter not available")
            return []

        symbols = symbols or self.earnings_symbols
        all_earnings = []

        # Date range for earnings calendar
        from_date = datetime.now(UTC).strftime("%Y-%m-%d")
        to_date = (datetime.now(UTC) + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

        for symbol in symbols:
            try:
                await self._rate_limit()

                logger.debug(f"Fetching earnings for {symbol}")
                result = self.adapter.fetch(
                    data_type="earnings",
                    symbol=symbol,
                    from_date=from_date,
                    to_date=to_date
                )

                if result and result.get("data"):
                    all_earnings.extend(result["data"])
                    logger.info(f"✅ Collected {len(result['data'])} earnings events for {symbol}")

            except Exception as e:
                logger.error(f"❌ Error collecting earnings for {symbol}: {e}")
                continue

        logger.info(f"📅 Collected {len(all_earnings)} total earnings events")
        return all_earnings

    async def collect_economic_indicators(
        self,
        countries: Optional[list[str]] = None,
        days_ahead: int = 7
    ) -> list[dict[str, Any]]:
        """Collect economic indicators.

        Args:
            countries: Countries to monitor (default: self.economic_countries)
            days_ahead: Days ahead to look for economic events

        Returns:
            List of economic events
        """
        if not self.adapter:
            logger.error("EODHD adapter not available")
            return []

        countries = countries or self.economic_countries
        all_events = []

        # Date range for economic calendar
        from_date = datetime.now(UTC).strftime("%Y-%m-%d")
        to_date = (datetime.now(UTC) + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

        for country in countries:
            try:
                await self._rate_limit()

                logger.debug(f"Fetching economic indicators for {country}")
                result = self.adapter.fetch(
                    data_type="economic",
                    country=country,
                    from_date=from_date,
                    to_date=to_date
                )

                if result and result.get("data"):
                    all_events.extend(result["data"])
                    logger.info(f"✅ Collected {len(result['data'])} economic events for {country}")

            except Exception as e:
                logger.error(f"❌ Error collecting economic data for {country}: {e}")
                continue

        logger.info(f"📊 Collected {len(all_events)} total economic events")
        return all_events

    async def collect_insider_transactions(
        self,
        symbols: Optional[list[str]] = None,
        limit: int = 50
    ) -> list[dict[str, Any]]:
        """Collect insider transaction data.

        Args:
            symbols: Symbols to monitor (default: self.symbols[:5])
            limit: Maximum transactions per symbol

        Returns:
            List of insider transactions
        """
        if not self.adapter:
            logger.error("EODHD adapter not available")
            return []

        symbols = symbols or self.symbols[:5]  # Top 5 symbols only
        all_transactions = []

        for symbol in symbols:
            try:
                await self._rate_limit()

                logger.debug(f"Fetching insider transactions for {symbol}")
                result = self.adapter.fetch(
                    data_type="insider_transactions",
                    symbol=symbol,
                    limit=limit
                )

                if result and result.get("data"):
                    all_transactions.extend(result["data"])
                    logger.info(f"✅ Collected {len(result['data'])} insider transactions for {symbol}")

            except Exception as e:
                logger.error(f"❌ Error collecting insider data for {symbol}: {e}")
                continue

        logger.info(f"🏢 Collected {len(all_transactions)} total insider transactions")
        return all_transactions

    async def run_collection_cycle(self) -> dict[str, Any]:
        """Run a complete fundamentals collection cycle.

        Returns:
            Collection results summary
        """
        logger.info("🚀 Starting fundamentals collection cycle")
        start_time = datetime.now(UTC)

        results = {
            "start_time": start_time.isoformat(),
            "fundamentals": [],
            "earnings": [],
            "economic": [],
            "insider_transactions": [],
            "errors": []
        }

        # 1. Collect fundamentals data
        logger.info(f"📊 Collecting fundamentals for {len(self.symbols)} symbols")
        for symbol in self.symbols:
            try:
                fundamental_data = await self.collect_fundamentals(symbol)
                if fundamental_data:
                    results["fundamentals"].append(fundamental_data)
            except Exception as e:
                error_msg = f"Fundamentals error for {symbol}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)

        # 2. Collect earnings calendar
        logger.info("📅 Collecting earnings calendar")
        try:
            earnings_data = await self.collect_earnings_calendar()
            results["earnings"] = earnings_data
        except Exception as e:
            error_msg = f"Earnings collection error: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        # 3. Collect economic indicators
        logger.info("🌍 Collecting economic indicators")
        try:
            economic_data = await self.collect_economic_indicators()
            results["economic"] = economic_data
        except Exception as e:
            error_msg = f"Economic data collection error: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        # 4. Collect insider transactions (optional, less frequent)
        if datetime.now().hour % 6 == 0:  # Every 6 hours
            logger.info("🏢 Collecting insider transactions")
            try:
                insider_data = await self.collect_insider_transactions()
                results["insider_transactions"] = insider_data
            except Exception as e:
                error_msg = f"Insider transactions error: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)

        # Collection summary
        end_time = datetime.now(UTC)
        duration = (end_time - start_time).total_seconds()

        results.update({
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_fundamentals": len(results["fundamentals"]),
            "total_earnings": len(results["earnings"]),
            "total_economic": len(results["economic"]),
            "total_insider": len(results["insider_transactions"]),
            "total_errors": len(results["errors"])
        })

        logger.info(
            f"✅ Collection cycle complete in {duration:.1f}s: "
            f"{results['total_fundamentals']} fundamentals, "
            f"{results['total_earnings']} earnings, "
            f"{results['total_economic']} economic events, "
            f"{results['total_insider']} insider transactions, "
            f"{results['total_errors']} errors"
        )

        return results

    async def _rate_limit(self):
        """Apply rate limiting between requests."""
        now = time.time()
        time_since_last = now - self.last_request_time

        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            await asyncio.sleep(sleep_time)

        self.last_request_time = time.time()

    async def start_continuous_collection(self):
        """Start continuous collection loop."""
        logger.info(f"Starting continuous fundamentals collection (interval: {self.collection_interval}h)")

        while True:
            try:
                await self.run_collection_cycle()

                # TODO: Store results in database/cache
                # This will be implemented in Phase 5.3 (TimescaleDB schema)

                # Wait for next collection cycle
                sleep_hours = self.collection_interval
                logger.info(f"😴 Sleeping for {sleep_hours} hours until next collection")
                await asyncio.sleep(sleep_hours * 3600)

            except KeyboardInterrupt:
                logger.info("🛑 Collection stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in collection loop: {e}")
                # Wait 1 hour before retrying on error
                await asyncio.sleep(3600)


async def main():
    """CLI entry point for fundamentals collector."""
    import argparse

    parser = argparse.ArgumentParser(description="FKS Fundamentals Data Collector")
    parser.add_argument("--symbols", nargs="+", help="Symbols to collect")
    parser.add_argument("--continuous", action="store_true", help="Run continuous collection")
    parser.add_argument("--earnings-only", action="store_true", help="Collect earnings only")
    parser.add_argument("--economic-only", action="store_true", help="Collect economic data only")

    args = parser.parse_args()

    # Initialize collector
    collector = FundamentalsCollector(symbols=args.symbols)

    if args.continuous:
        await collector.start_continuous_collection()
    elif args.earnings_only:
        earnings = await collector.collect_earnings_calendar()
        print(f"Collected {len(earnings)} earnings events")
    elif args.economic_only:
        economic = await collector.collect_economic_indicators()
        print(f"Collected {len(economic)} economic events")
    else:
        # Single collection cycle
        results = await collector.run_collection_cycle()
        print(f"Collection complete: {results['total_fundamentals']} fundamentals collected")


if __name__ == "__main__":
    asyncio.run(main())
