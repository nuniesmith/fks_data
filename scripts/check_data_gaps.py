#!/usr/bin/env python3
"""Script to check for data gaps in OHLCV data."""

import os
import sys
import psycopg2
from datetime import timedelta

# Database connection
DB_HOST = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "fks_data_db"))
DB_PORT = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
DB_NAME = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "trading_db"))
DB_USER = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "fks_user"))
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "fks_password"))

def check_gaps():
    """Check for data gaps in OHLCV data."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    try:
        cur = conn.cursor()
        
        # Check gaps by symbol
        query = """
        WITH time_series AS (
            SELECT 
                symbol,
                interval,
                ts,
                LAG(ts) OVER (PARTITION BY symbol, interval ORDER BY ts) as prev_ts,
                ts - LAG(ts) OVER (PARTITION BY symbol, interval ORDER BY ts) as gap
            FROM ohlcv
            WHERE interval = '1h'
        )
        SELECT 
            symbol,
            interval,
            COUNT(*) as total_candles,
            COUNT(CASE WHEN gap > INTERVAL '1 hour 10 minutes' THEN 1 END) as gaps_count,
            MAX(gap) as max_gap,
            MIN(ts) as earliest,
            MAX(ts) as latest
        FROM time_series
        WHERE prev_ts IS NOT NULL
        GROUP BY symbol, interval
        ORDER BY symbol, interval;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        print("Data Gap Analysis")
        print("=" * 80)
        print(f"{'Symbol':<12} {'Interval':<10} {'Candles':<10} {'Gaps':<8} {'Max Gap':<20} {'Earliest':<20} {'Latest':<20}")
        print("-" * 80)
        
        total_gaps = 0
        for row in results:
            symbol, interval, candles, gaps, max_gap, earliest, latest = row
            total_gaps += gaps or 0
            max_gap_str = str(max_gap) if max_gap else "N/A"
            earliest_str = earliest.strftime("%Y-%m-%d %H:%M") if earliest else "N/A"
            latest_str = latest.strftime("%Y-%m-%d %H:%M") if latest else "N/A"
            
            gap_indicator = "⚠️" if gaps and gaps > 0 else "✅"
            print(f"{symbol:<12} {interval:<10} {candles:<10} {gap_indicator} {gaps or 0:<6} {max_gap_str:<20} {earliest_str:<20} {latest_str:<20}")
        
        print("-" * 80)
        print(f"Total gaps found: {total_gaps}")
        
        if total_gaps == 0:
            print("✅ No data gaps detected!")
            return 0
        else:
            print(f"⚠️ Found {total_gaps} data gaps")
            return 1
            
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    sys.exit(check_gaps())

