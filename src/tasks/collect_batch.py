"""Batch collection task for multiple symbols."""

import logging
from typing import List, Optional

from celery import group
from celery.utils.log import get_task_logger

from src.celery_app import celery_app
from src.tasks.collect_data import collect_ohlcv_data

logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)


@celery_app.task(name="src.tasks.collect_batch.collect_multiple_symbols")
def collect_multiple_symbols(
    symbols: List[str],
    interval: str = "1h",
    limit: int = 1,
    provider: Optional[str] = None,
):
    """
    Collect OHLCV data for multiple symbols in parallel.
    
    Args:
        symbols: List of trading symbols (e.g., ["BTCUSDT", "ETHUSDT"])
        interval: Time interval (1m, 5m, 1h, 1d)
        limit: Number of candles to fetch per symbol
        provider: Specific provider to use (optional)
    
    Returns:
        dict: Summary of collection results
    """
    logger.info(f"🔄 Starting batch collection for {len(symbols)} symbols: {symbols}")
    
    # Create a group of tasks to run in parallel
    job = group(
        collect_ohlcv_data.s(symbol, interval, limit, provider)
        for symbol in symbols
    )
    
    # Execute the group
    result = job.apply_async()
    
    # Wait for all tasks to complete (with timeout)
    try:
        results = result.get(timeout=300)  # 5 minute timeout
        success_count = sum(1 for r in results if r and r.get("status") == "success")
        failure_count = len(results) - success_count
        
        summary = {
            "status": "completed",
            "total_symbols": len(symbols),
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results,
        }
        
        logger.info(f"✅ Batch collection completed: {success_count}/{len(symbols)} successful")
        return summary
        
    except Exception as e:
        logger.error(f"❌ Batch collection failed: {e}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "total_symbols": len(symbols),
        }

