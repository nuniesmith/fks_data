"""Background tasks for collecting market data."""

import logging
import os
from datetime import datetime
from typing import Optional

import psycopg2
from celery import Task
from celery.utils.log import get_task_logger

# Import celery_app first to avoid circular imports
from src.celery_app import celery_app
from adapters.multi_provider_manager import MultiProviderManager

logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)


class DatabaseTask(Task):
    """Base task class with database connection handling."""
    
    _db_conn = None
    
    def get_db_connection(self):
        """Get or create database connection."""
        if self._db_conn is None or self._db_conn.closed:
            db_host = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "fks_data_db"))
            db_port = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
            db_name = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "trading_db"))
            db_user = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "fks_user"))
            db_password = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "fks_password"))
            
            self._db_conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            logger.info(f"Database connection established to {db_host}:{db_port}/{db_name}")
        return self._db_conn
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure."""
        logger.error(f"Task {task_id} failed: {exc}", exc_info=einfo)
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Handle task retry."""
        logger.warning(f"Task {task_id} retrying: {exc}")


def store_ohlcv_data(symbol: str, interval: str, data_list: list, provider: str = "binance"):
    """Store OHLCV data in database."""
    try:
        db_host = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "fks_data_db"))
        db_port = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
        db_name = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "trading_db"))
        db_user = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "fks_user"))
        db_password = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "fks_password"))
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        try:
            cur = conn.cursor()
            
            # Prepare data for insertion
            rows = []
            for item in data_list:
                ts_value = item.get("ts", 0)
                if isinstance(ts_value, (int, float)):
                    from datetime import datetime
                    ts_value = datetime.fromtimestamp(ts_value)
                elif isinstance(ts_value, str):
                    from dateutil import parser
                    ts_value = parser.parse(ts_value)
                
                rows.append((
                    provider,
                    symbol,
                    interval,
                    ts_value,
                    item.get("open"),
                    item.get("high"),
                    item.get("low"),
                    item.get("close"),
                    item.get("volume")
                ))
            
            if rows:
                sql = (
                    "INSERT INTO ohlcv (source, symbol, interval, ts, open, high, low, close, volume) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (source, symbol, interval, ts) DO UPDATE SET "
                    "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume"
                )
                cur.executemany(sql, rows)
                conn.commit()
                logger.info(f"✅ Stored {len(rows)} rows for {symbol} {interval} from {provider}")
                return len(rows)
            return 0
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.error(f"❌ Failed to store data in database: {e}", exc_info=True)
        raise


# Register the task with Celery
@celery_app.task(
    base=DatabaseTask,
    bind=True,
    name="src.tasks.collect_data.collect_ohlcv_data",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,  # Max 10 minutes
    retry_jitter=True,
    max_retries=3,
)
def collect_ohlcv_data(self, symbol: str, interval: str = "1h", limit: int = 1, provider: Optional[str] = None):
    """
    Collect OHLCV data from exchange and store in database.
    
    Args:
        symbol: Trading symbol (e.g., BTCUSDT, ETHUSDT)
        interval: Time interval (1m, 5m, 1h, 1d)
        limit: Number of candles to fetch
        provider: Specific provider to use (optional)
    
    Returns:
        dict: Collection result with status and statistics
    """
    task_id = self.request.id
    logger.info(f"🔵 Starting data collection task {task_id}: {symbol} {interval} (limit={limit})")
    
    try:
        # Use MultiProviderManager for failover
        manager = MultiProviderManager()
        
        # Fetch data
        result = manager.get_data(
            asset=symbol,
            granularity=interval,
            providers=[provider] if provider else None,
            limit=limit
        )
        
        if not result or not result.get("data"):
            error_msg = f"No data returned for {symbol} {interval}"
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        data_list = result["data"]
        provider_name = result.get("provider", "unknown")
        
        logger.info(f"📊 Fetched {len(data_list)} candles for {symbol} {interval} from {provider_name}")
        
        # Store in database
        stored_count = store_ohlcv_data(symbol, interval, data_list, provider_name)
        
        result_summary = {
            "status": "success",
            "task_id": task_id,
            "symbol": symbol,
            "interval": interval,
            "provider": provider_name,
            "candles_fetched": len(data_list),
            "candles_stored": stored_count,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        logger.info(f"✅ Task {task_id} completed successfully: {result_summary}")
        return result_summary
        
    except Exception as exc:
        logger.error(f"❌ Task {task_id} failed: {exc}", exc_info=True)
        # Re-raise to trigger retry
        raise

