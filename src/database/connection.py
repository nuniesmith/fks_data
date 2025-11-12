"""
Database Connection Utilities

Provides database connection management for quality metrics storage.
Phase: 5.6 Task 3 - Pipeline Integration
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator

try:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None
    Json = None

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection() -> Generator:
    """
    Context manager for database connections.

    Yields:
        Database connection with auto-commit on success, rollback on error

    Example:
        >>> with get_db_connection() as conn:
        ...     with conn.cursor() as cur:
        ...         cur.execute("SELECT * FROM quality_metrics LIMIT 1")
        ...         result = cur.fetchone()
    """
    if psycopg2 is None:
        raise ImportError("psycopg2 is required for database connections")

    # Get connection parameters from environment
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'db'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'database': os.getenv('POSTGRES_DB', 'trading_db'),
        'user': os.getenv('POSTGRES_USER', 'fks_user'),
        'password': os.getenv('POSTGRES_PASSWORD', 'fks_password'),
    }

    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Database connection error: %s", e)
        raise
    finally:
        if conn:
            conn.close()


def execute_query(query: str, params: dict = None, fetch: bool = True):
    """
    Execute a database query with automatic connection management.

    Args:
        query: SQL query string
        params: Query parameters (optional)
        fetch: Whether to fetch results (default: True)

    Returns:
        Query results if fetch=True, else None

    Example:
        >>> results = execute_query(
        ...     "SELECT * FROM quality_metrics WHERE symbol = %(symbol)s",
        ...     {'symbol': 'BTCUSDT'}
        ... )
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or {})

            if fetch:
                results = cur.fetchall()
                conn.commit()
                return results
            else:
                conn.commit()
                return None


def insert_quality_metric(data: dict) -> None:
    """
    Insert a quality metric record into TimescaleDB.

    Args:
        data: Dictionary with quality metric fields

    Example:
        >>> data = {
        ...     'time': datetime.now(),
        ...     'symbol': 'BTCUSDT',
        ...     'overall_score': 85.0,
        ...     'status': 'good',
        ...     'issues': [{'type': 'outlier', 'severity': 'low'}],
        ...     'issue_count': 1
        ... }
        >>> insert_quality_metric(data)
    """
    # Convert issues list to JSONB
    if 'issues' in data and isinstance(data['issues'], list):
        data['issues'] = Json(data['issues'])

    query = """
        INSERT INTO quality_metrics (
            time, symbol, overall_score, status,
            outlier_score, freshness_score, completeness_score,
            outlier_count, outlier_severity,
            freshness_age_seconds, completeness_percentage,
            issues, issue_count, check_duration_ms, collector_version
        ) VALUES (
            %(time)s, %(symbol)s, %(overall_score)s, %(status)s,
            %(outlier_score)s, %(freshness_score)s, %(completeness_score)s,
            %(outlier_count)s, %(outlier_severity)s,
            %(freshness_age_seconds)s, %(completeness_percentage)s,
            %(issues)s, %(issue_count)s, %(check_duration_ms)s, %(collector_version)s
        )
    """

    execute_query(query, data, fetch=False)
    logger.debug("Inserted quality metric for symbol: %s", data.get('symbol'))


def get_latest_quality_score(symbol: str) -> dict:
    """
    Get the latest quality score for a symbol.

    Args:
        symbol: Trading pair symbol

    Returns:
        Dictionary with latest quality metrics, or None if not found
    """
    query = """
        SELECT * FROM quality_metrics
        WHERE symbol = %(symbol)s
        ORDER BY time DESC
        LIMIT 1
    """

    results = execute_query(query, {'symbol': symbol})
    return results[0] if results else None


def get_quality_history(symbol: str, hours: int = 24) -> list:
    """
    Get quality score history for a symbol.

    Args:
        symbol: Trading pair symbol
        hours: Number of hours of history to retrieve

    Returns:
        List of quality metric records
    """
    query = """
        SELECT * FROM quality_metrics
        WHERE symbol = %(symbol)s
        AND time > NOW() - INTERVAL '%(hours)s hours'
        ORDER BY time DESC
    """

    return execute_query(query, {'symbol': symbol, 'hours': hours})


def get_quality_statistics(symbol: str = None, hours: int = 24) -> dict:
    """
    Get aggregated quality statistics.

    Args:
        symbol: Trading pair symbol (optional, None for all symbols)
        hours: Number of hours to aggregate

    Returns:
        Dictionary with aggregated statistics
    """
    where_clause = "WHERE symbol = %(symbol)s AND" if symbol else "WHERE"

    query = f"""
        SELECT
            {f"'{symbol}' as symbol," if symbol else "symbol,"}
            AVG(overall_score) as avg_score,
            MIN(overall_score) as min_score,
            MAX(overall_score) as max_score,
            STDDEV(overall_score) as stddev_score,
            SUM(issue_count) as total_issues,
            COUNT(*) as check_count
        FROM quality_metrics
        {where_clause} time > NOW() - INTERVAL '%(hours)s hours'
        {"" if symbol else "GROUP BY symbol"}
    """

    params = {'hours': hours}
    if symbol:
        params['symbol'] = symbol

    results = execute_query(query, params)
    return results[0] if results else {}
