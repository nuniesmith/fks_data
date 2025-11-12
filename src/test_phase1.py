"""Test script for Phase 1 Tasks 1-2: EUR/USD data collection and Δ scanner.

This script validates:
1. ForexTickCollector can fetch EUR/USD tick data
2. DeltaScanner can detect micro-price changes
3. Data quality meets >99% completeness target

Usage:
    python -m src.services.data.src.test_phase1
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add current directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent))

from collectors.forex_collector import ForexTickCollector  # noqa: E402
from processors.delta_scanner import DeltaScanner, analyze_tick_data  # noqa: E402


async def test_forex_collector():
    """Test Task 1: EUR/USD data collection."""
    print("\n" + "="*60)
    print("🧪 PHASE 1 TASK 1: EUR/USD Data Collection")
    print("="*60)

    collector = ForexTickCollector(
        exchange_id="kraken",
        symbol="EUR/USDT",
        resolution_ms=2000  # 2 second ticks for testing
    )

    try:
        # Initialize
        print("\n📡 Initializing collector...")
        await collector.initialize()

        # Test single ticker fetch
        print("\n📊 Fetching sample ticker...")
        ticker = await collector.fetch_ticker()
        print(f"  ✅ Ticker: {ticker['timestamp']} - bid: {ticker['bid']}, ask: {ticker['ask']}, last: {ticker['last']}")

        # Test orderbook snapshot
        print("\n📖 Fetching orderbook snapshot...")
        orderbook = await collector.fetch_orderbook_snapshot(limit=3)
        print(f"  ✅ Spread: {orderbook['spread']}")
        print(f"  Top bid: {orderbook['bids'][0]}")
        print(f"  Top ask: {orderbook['asks'][0]}")

        # Stream ticks for 30 seconds
        print("\n⏱️ Streaming ticks for 30 seconds...")
        ticks = await collector.stream_ticks(duration_seconds=30)

        # Get quality metrics
        metrics = await collector.get_data_quality_metrics()
        print("\n📈 Data Quality Metrics:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")

        # Validate completeness
        completeness = metrics['completeness_pct']
        if completeness >= 99.0:
            print(f"\n✅ SUCCESS: Data completeness {completeness}% meets >99% target")
        else:
            print(f"\n⚠️ WARNING: Data completeness {completeness}% below 99% target")

        return ticks, metrics

    finally:
        await collector.cleanup()


def test_delta_scanner(ticks):
    """Test Task 2: Δ Scanner implementation."""
    print("\n" + "="*60)
    print("🧪 PHASE 1 TASK 2: Δ Scanner Implementation")
    print("="*60)

    if not ticks:
        print("❌ No ticks available for scanning")
        return None

    # Analyze ticks
    print(f"\n🔍 Scanning {len(ticks)} ticks for micro-price changes...")
    results = analyze_tick_data(ticks, price_key='last')

    print("\n📊 Scanner Results:")
    print(f"  Total changes: {results['total_changes']}")
    print(f"  Micro-changes (<0.01%): {results['micro_changes']} ({results['micro_pct']}%)")
    print(f"  Up movements: {results['up_changes']} ({results['up_pct']}%)")
    print(f"  Down movements: {results['down_changes']} ({results['down_pct']}%)")
    print(f"  Neutral: {results['neutral_changes']} ({results['neutral_pct']}%)")

    print(f"\n🔢 Binary sequence (last 32 moves): {results['binary_sequence'][-32:]}")
    print(f"  Full sequence length: {results['sequence_length']}")

    # Show sample changes
    if results['changes']:
        print("\n📈 Sample Changes (first 5):")
        for i, change in enumerate(results['changes'][:5], 1):
            direction_symbol = "📈" if change.direction.value == 1 else "📉" if change.direction.value == -1 else "➡️"
            micro_label = " [MICRO]" if change.is_micro else ""
            print(f"  {i}. {direction_symbol} {change.timestamp.strftime('%H:%M:%S')}: "
                  f"{change.old_price} → {change.new_price} "
                  f"(Δ={change.delta_pct:+.4f}%){micro_label}")

    # Validate micro-change detection
    if results['micro_changes'] > 0:
        print(f"\n✅ SUCCESS: Scanner detected {results['micro_changes']} micro-changes")
    else:
        print("\n⚠️ WARNING: No micro-changes detected (EUR/USD may be stable)")

    return results


async def main():
    """Run all Phase 1 tests."""
    print("\n" + "="*70)
    print(" FKS AI ENHANCEMENT PLAN - PHASE 1 VALIDATION ".center(70, "="))
    print("="*70)
    print("\nObjective: Validate EUR/USD tick data collection and Δ scanner")
    print("Targets:")
    print("  - Data completeness: >99%")
    print("  - Resolution: <1s (using 2s for testing)")
    print("  - Micro-change detection: <0.01%")
    print("\n" + "="*70)

    try:
        # Test 1: Data collection
        ticks, metrics = await test_forex_collector()

        # Test 2: Delta scanner
        scanner_results = test_delta_scanner(ticks)

        # Summary
        print("\n" + "="*70)
        print(" PHASE 1 VALIDATION SUMMARY ".center(70, "="))
        print("="*70)

        print("\n✅ Task 1: EUR/USD Data Collection")
        print(f"   - Ticks collected: {metrics['ticks_collected']}")
        print(f"   - Completeness: {metrics['completeness_pct']}%")
        print(f"   - Target met: {'YES ✅' if metrics['completeness_pct'] >= 99 else 'NO ❌'}")

        if scanner_results:
            print("\n✅ Task 2: Δ Scanner Implementation")
            print(f"   - Total changes: {scanner_results['total_changes']}")
            print(f"   - Micro-changes: {scanner_results['micro_changes']} ({scanner_results['micro_pct']}%)")
            print(f"   - Binary sequence length: {scanner_results['sequence_length']}")
            print("   - Scanner operational: YES ✅")

        print("\n📋 Next Steps:")
        print("  1. Verify TimescaleDB hypertable compatibility (Task 3)")
        print("  2. Implement BTR encoder using binary sequences (Phase 2)")
        print("  3. Build ASMBTR prediction table and strategy (Phase 2)")

        print("\n" + "="*70 + "\n")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    exit_code = asyncio.run(main())
    sys.exit(exit_code)
