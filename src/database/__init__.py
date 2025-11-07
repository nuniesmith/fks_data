"""
Database module for quality metrics storage

Provides database connection utilities and query functions for TimescaleDB.
Phase: 5.6 Task 3 - Pipeline Integration
"""

from database.connection import (
    execute_query,
    get_db_connection,
    get_latest_quality_score,
    get_quality_history,
    get_quality_statistics,
    insert_quality_metric,
)

__all__ = [
    'get_db_connection',
    'execute_query',
    'insert_quality_metric',
    'get_latest_quality_score',
    'get_quality_history',
    'get_quality_statistics',
]
