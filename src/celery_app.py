"""Celery application configuration for fks_data background tasks."""

import os
import logging

from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger(__name__)

# Redis URL from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://:@fks_data_redis:6379/0")

# Create Celery app
celery_app = Celery(
    "fks_data",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes max per task
    task_soft_time_limit=240,  # 4 minutes soft limit
    worker_prefetch_multiplier=1,  # Disable prefetching for better task distribution
    worker_max_tasks_per_child=100,  # Restart worker after 100 tasks
    task_acks_late=True,  # Acknowledge tasks after completion
    task_reject_on_worker_lost=True,  # Reject tasks if worker dies
    task_default_retry_delay=60,  # Default retry delay: 60 seconds
    task_max_retries=3,  # Max 3 retries
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
)

# Import tasks to register them
try:
    import src.tasks.collect_data  # noqa: F401
    import src.tasks.collect_batch  # noqa: F401
except ImportError as e:
    logger.warning(f"Failed to import tasks module: {e}")

# Default symbols to collect
# MAINS: Primary cryptocurrencies
# ALTS: Alternative cryptocurrencies
DEFAULT_SYMBOLS = {
    "MAINS": ["BTCUSDT", "ETHUSDT"],
    "ALTS": ["SOLUSDT", "AVAXUSDT", "SUIUSDT"],
}

# Beat schedule - collect data every 5 minutes
# Stagger tasks slightly to avoid all hitting at once
beat_schedule = {}

# Add main symbols (every 5 minutes)
for idx, symbol in enumerate(DEFAULT_SYMBOLS["MAINS"]):
    beat_schedule[f"collect-{symbol.lower()}-1h"] = {
        "task": "src.tasks.collect_data.collect_ohlcv_data",
        "schedule": 300.0,  # Every 5 minutes
        "args": (symbol, "1h", 1),
    }

# Add alt symbols (every 5 minutes, slightly staggered)
for idx, symbol in enumerate(DEFAULT_SYMBOLS["ALTS"]):
    beat_schedule[f"collect-{symbol.lower()}-1h"] = {
        "task": "src.tasks.collect_data.collect_ohlcv_data",
        "schedule": 300.0,  # Every 5 minutes
        "args": (symbol, "1h", 1),
    }

celery_app.conf.beat_schedule = beat_schedule

logger.info(f"Configured beat schedule with {len(beat_schedule)} tasks for symbols: {DEFAULT_SYMBOLS['MAINS'] + DEFAULT_SYMBOLS['ALTS']}")

logger.info("Celery app configured with broker: %s", REDIS_URL.split("@")[-1] if "@" in REDIS_URL else REDIS_URL)

