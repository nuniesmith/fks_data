#!/usr/bin/env python3
"""Initialize database schema for fks_data service.

This script creates the necessary tables and TimescaleDB hypertables.
Run this after the database container is up and ready.
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from store import ensure_schema
from loguru import logger

def main():
    """Initialize database schema."""
    logger.info("Initializing database schema...")
    
    try:
        ensure_schema()
        logger.info("✅ Database schema initialized successfully")
        
        # Also create TimescaleDB hypertable if TimescaleDB is available
        try:
            from infrastructure.database.postgres import get_connection
            
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if TimescaleDB extension exists
                    cur.execute("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')")
                    has_timescaledb = cur.fetchone()[0]
                    
                    if has_timescaledb:
                        logger.info("TimescaleDB extension found. Creating hypertable...")
                        # Convert ohlcv table to hypertable if not already
                        cur.execute("""
                            SELECT create_hypertable('ohlcv', 'ts', if_not_exists => TRUE)
                        """)
                        logger.info("✅ Hypertable created successfully")
                    else:
                        logger.warning("TimescaleDB extension not found. Using regular PostgreSQL tables.")
                
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not create hypertable: {e}")
            logger.info("Continuing with regular PostgreSQL tables...")
        
        return 0
    except Exception as e:
        logger.error(f"❌ Failed to initialize schema: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

