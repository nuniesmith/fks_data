#!/usr/bin/env python3
"""Monitor data collection metrics and health."""

import os
import sys
import psycopg2
from datetime import datetime, timedelta
import json

# Database connection
DB_HOST = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "fks_data_db"))
DB_PORT = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
DB_NAME = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "trading_db"))
DB_USER = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "fks_user"))
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "fks_password"))


def get_collection_metrics():
    """Get collection metrics from database."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    try:
        cur = conn.cursor()
        
        # Overall statistics
        cur.execute("""
            SELECT 
                COUNT(DISTINCT symbol) as total_symbols,
                COUNT(*) as total_candles,
                MIN(ts) as earliest,
                MAX(ts) as latest
            FROM ohlcv
            WHERE interval = '1h'
        """)
        overall = cur.fetchone()
        
        # Per-symbol statistics
        cur.execute("""
            SELECT 
                symbol,
                COUNT(*) as candle_count,
                MIN(ts) as first_collection,
                MAX(ts) as last_collection,
                EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 3600 as hours_span,
                EXTRACT(EPOCH FROM (NOW() - MAX(ts))) / 60 as minutes_since_last
            FROM ohlcv
            WHERE interval = '1h'
            GROUP BY symbol
            ORDER BY symbol
        """)
        per_symbol = cur.fetchall()
        
        # Recent activity (last hour)
        cur.execute("""
            SELECT 
                symbol,
                COUNT(*) as recent_count
            FROM ohlcv
            WHERE interval = '1h'
              AND ts > NOW() - INTERVAL '1 hour'
            GROUP BY symbol
            ORDER BY symbol
        """)
        recent = {row[0]: row[1] for row in cur.fetchall()}
        
        # Gap detection
        cur.execute("""
            WITH time_series AS (
                SELECT 
                    symbol,
                    ts,
                    LAG(ts) OVER (PARTITION BY symbol ORDER BY ts) as prev_ts,
                    ts - LAG(ts) OVER (PARTITION BY symbol ORDER BY ts) as gap
                FROM ohlcv
                WHERE interval = '1h'
            )
            SELECT 
                symbol,
                COUNT(CASE WHEN gap > INTERVAL '1 hour 10 minutes' THEN 1 END) as gaps_count,
                MAX(gap) as max_gap
            FROM time_series
            WHERE prev_ts IS NOT NULL
            GROUP BY symbol
            ORDER BY symbol
        """)
        gaps = {row[0]: {"count": row[1] or 0, "max_gap": str(row[2]) if row[2] else None} for row in cur.fetchall()}
        
        return {
            "overall": {
                "total_symbols": overall[0],
                "total_candles": overall[1],
                "earliest": overall[2].isoformat() if overall[2] else None,
                "latest": overall[3].isoformat() if overall[3] else None,
            },
            "per_symbol": [
                {
                    "symbol": row[0],
                    "candle_count": row[1],
                    "first_collection": row[2].isoformat() if row[2] else None,
                    "last_collection": row[3].isoformat() if row[3] else None,
                    "hours_span": float(row[4]) if row[4] else 0,
                    "minutes_since_last": float(row[5]) if row[5] else None,
                    "recent_count": recent.get(row[0], 0),
                    "gaps": gaps.get(row[0], {"count": 0, "max_gap": None}),
                }
                for row in per_symbol
            ],
            "timestamp": datetime.utcnow().isoformat(),
        }
        
    finally:
        cur.close()
        conn.close()


def print_metrics(metrics):
    """Print metrics in human-readable format."""
    print("=" * 80)
    print("Data Collection Metrics")
    print("=" * 80)
    print(f"Timestamp: {metrics['timestamp']}")
    print()
    
    overall = metrics["overall"]
    print(f"Overall Statistics:")
    print(f"  Total Symbols: {overall['total_symbols']}")
    print(f"  Total Candles: {overall['total_candles']}")
    print(f"  Earliest: {overall['earliest']}")
    print(f"  Latest: {overall['latest']}")
    print()
    
    print("Per-Symbol Statistics:")
    print("-" * 80)
    print(f"{'Symbol':<12} {'Candles':<10} {'Last Collection':<20} {'Age':<10} {'Recent':<8} {'Gaps':<6}")
    print("-" * 80)
    
    for symbol_data in metrics["per_symbol"]:
        age = f"{symbol_data['minutes_since_last']:.1f}m" if symbol_data['minutes_since_last'] else "N/A"
        recent = symbol_data['recent_count']
        gaps = symbol_data['gaps']['count']
        gap_indicator = "✅" if gaps == 0 else f"⚠️ {gaps}"
        
        last_col = symbol_data['last_collection'][:19] if symbol_data['last_collection'] else "N/A"
        
        print(f"{symbol_data['symbol']:<12} {symbol_data['candle_count']:<10} {last_col:<20} {age:<10} {recent:<8} {gap_indicator:<6}")
    
    print("-" * 80)
    
    # Health check
    print()
    print("Health Status:")
    unhealthy = []
    for symbol_data in metrics["per_symbol"]:
        if symbol_data['minutes_since_last'] and symbol_data['minutes_since_last'] > 10:
            unhealthy.append(f"{symbol_data['symbol']} (last collection: {symbol_data['minutes_since_last']:.1f}m ago)")
        if symbol_data['gaps']['count'] > 0:
            unhealthy.append(f"{symbol_data['symbol']} ({symbol_data['gaps']['count']} gaps)")
    
    if not unhealthy:
        print("  ✅ All symbols healthy")
    else:
        print("  ⚠️ Issues detected:")
        for issue in unhealthy:
            print(f"    - {issue}")


def main():
    """Main function."""
    try:
        metrics = get_collection_metrics()
        
        if len(sys.argv) > 1 and sys.argv[1] == "--json":
            print(json.dumps(metrics, indent=2))
        else:
            print_metrics(metrics)
        
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

