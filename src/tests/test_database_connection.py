"""
Tests for database connection and quality metrics storage.

This module tests the TimescaleDB integration for quality metrics:
- Connection management
- Quality metric insertion
- Query functions (latest, history, statistics)
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from psycopg2.extras import Json, RealDictCursor


@pytest.fixture
def mock_connection():
    """Mock database connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture
def sample_quality_data():
    """Sample quality metric data for testing."""
    return {
        'time': datetime.now(),
        'symbol': 'BTCUSDT',
        'overall_score': 85.5,
        'status': 'good',
        'outlier_score': 90.0,
        'freshness_score': 95.0,
        'completeness_score': 98.5,
        'outlier_count': 2,
        'outlier_severity': 'minor',
        'freshness_age_seconds': 5.0,
        'completeness_percentage': 98.5,
        'issues': [
            {'type': 'outlier', 'severity': 'minor', 'message': 'Minor price spike detected'}
        ],
        'issue_count': 1,
        'check_duration_ms': 25.5,
        'collector_version': 'v1.0'
    }


class TestGetDbConnection:
    """Test database connection context manager."""

    @patch('database.connection.psycopg2.connect')
    @patch.dict('os.environ', {
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'trading_db',
        'POSTGRES_USER': 'fks_user',
        'POSTGRES_PASSWORD': 'test_password'
    })
    def test_connection_success(self, mock_connect):
        """Test successful database connection."""
        from database.connection import get_db_connection

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with get_db_connection() as conn:
            assert conn is mock_conn

        mock_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            dbname='trading_db',
            user='fks_user',
            password='test_password'
        )

    @patch('database.connection.psycopg2.connect')
    @patch.dict('os.environ', {
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'trading_db',
        'POSTGRES_USER': 'fks_user',
        'POSTGRES_PASSWORD': 'test_password'
    })
    def test_connection_failure(self, mock_connect):
        """Test database connection failure."""
        from database.connection import get_db_connection

        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")

        with pytest.raises(psycopg2.OperationalError, match="Connection failed"):
            with get_db_connection():
                pass


class TestInsertQualityMetric:
    """Test quality metric insertion."""

    @patch('database.connection.get_db_connection')
    def test_insert_success(self, mock_get_conn, sample_quality_data, mock_connection):
        """Test successful quality metric insertion."""
        from database.connection import insert_quality_metric

        conn, cursor = mock_connection
        mock_get_conn.return_value = conn

        insert_quality_metric(sample_quality_data)

        # Verify execute was called
        assert cursor.execute.called
        call_args = cursor.execute.call_args

        # Check SQL contains expected columns
        sql = call_args[0][0]
        assert 'INSERT INTO quality_metrics' in sql
        assert 'overall_score' in sql
        assert 'symbol' in sql

        # Check data was passed
        data = call_args[0][1]
        assert data['symbol'] == 'BTCUSDT'
        assert data['overall_score'] == 85.5

        # Verify commit was called
        conn.commit.assert_called_once()

    @patch('database.connection.get_db_connection')
    def test_insert_with_jsonb(self, mock_get_conn, sample_quality_data, mock_connection):
        """Test insertion with JSONB issues array."""
        from database.connection import insert_quality_metric

        conn, cursor = mock_connection
        mock_get_conn.return_value = conn

        insert_quality_metric(sample_quality_data)

        call_args = cursor.execute.call_args
        data = call_args[0][1]

        # Check issues were converted to Json
        assert isinstance(data['issues'], Json)
        assert data['issues'].adapted == sample_quality_data['issues']


class TestGetLatestQualityScore:
    """Test retrieving latest quality score."""

    @patch('database.connection.execute_query')
    def test_get_latest_found(self, mock_execute):
        """Test retrieving existing latest score."""
        from database.connection import get_latest_quality_score

        expected_result = {
            'time': datetime.now(),
            'symbol': 'BTCUSDT',
            'overall_score': 85.5,
            'status': 'good'
        }
        mock_execute.return_value = [expected_result]

        result = get_latest_quality_score('BTCUSDT')

        assert result == expected_result
        assert mock_execute.called
        call_args = mock_execute.call_args
        assert 'ORDER BY time DESC' in call_args[0][0]
        assert call_args[0][1] == {'symbol': 'BTCUSDT'}

    @patch('database.connection.execute_query')
    def test_get_latest_not_found(self, mock_execute):
        """Test when no records exist."""
        from database.connection import get_latest_quality_score

        mock_execute.return_value = []

        result = get_latest_quality_score('NONEXISTENT')

        assert result is None


class TestGetQualityHistory:
    """Test retrieving quality history."""

    @patch('database.connection.execute_query')
    def test_get_history_with_defaults(self, mock_execute):
        """Test history retrieval with default parameters."""
        from database.connection import get_quality_history

        expected_results = [
            {'time': datetime.now(), 'overall_score': 85.5},
            {'time': datetime.now() - timedelta(hours=1), 'overall_score': 80.0}
        ]
        mock_execute.return_value = expected_results

        results = get_quality_history('BTCUSDT')

        assert results == expected_results
        assert mock_execute.called
        call_args = mock_execute.call_args
        sql = call_args[0][0]
        assert 'WHERE symbol = %(symbol)s' in sql
        assert 'ORDER BY time DESC' in sql

    @patch('database.connection.execute_query')
    def test_get_history_with_time_range(self, mock_execute):
        """Test history with specific time range."""
        from database.connection import get_quality_history

        mock_execute.return_value = []

        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()

        get_quality_history('BTCUSDT', start_time=start_time, end_time=end_time)

        call_args = mock_execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert 'time >= %(start_time)s' in sql
        assert 'time <= %(end_time)s' in sql
        assert params['start_time'] == start_time
        assert params['end_time'] == end_time

    @patch('database.connection.execute_query')
    def test_get_history_with_limit(self, mock_execute):
        """Test history with row limit."""
        from database.connection import get_quality_history

        mock_execute.return_value = []

        get_quality_history('BTCUSDT', limit=50)

        call_args = mock_execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert 'LIMIT %(limit)s' in sql
        assert params['limit'] == 50


class TestGetQualityStatistics:
    """Test quality statistics aggregation."""

    @patch('database.connection.execute_query')
    def test_get_statistics_found(self, mock_execute):
        """Test statistics retrieval with data."""
        from database.connection import get_quality_statistics

        expected_stats = {
            'avg_score': 85.5,
            'min_score': 65.0,
            'max_score': 98.0,
            'stddev_score': 8.5,
            'count': 100
        }
        mock_execute.return_value = [expected_stats]

        result = get_quality_statistics('BTCUSDT')

        assert result == expected_stats
        assert mock_execute.called
        call_args = mock_execute.call_args
        sql = call_args[0][0]

        assert 'AVG(overall_score)' in sql
        assert 'MIN(overall_score)' in sql
        assert 'MAX(overall_score)' in sql
        assert 'STDDEV(overall_score)' in sql
        assert 'COUNT(*)' in sql

    @patch('database.connection.execute_query')
    def test_get_statistics_with_time_range(self, mock_execute):
        """Test statistics with time range filter."""
        from database.connection import get_quality_statistics

        mock_execute.return_value = [{}]

        start_time = datetime.now() - timedelta(days=30)

        get_quality_statistics('BTCUSDT', start_time=start_time)

        call_args = mock_execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert 'time >= %(start_time)s' in sql
        assert params['start_time'] == start_time

    @patch('database.connection.execute_query')
    def test_get_statistics_not_found(self, mock_execute):
        """Test when no statistics exist."""
        from database.connection import get_quality_statistics

        mock_execute.return_value = []

        result = get_quality_statistics('NONEXISTENT')

        assert result is None


class TestExecuteQuery:
    """Test generic query execution."""

    @patch('database.connection.get_db_connection')
    def test_execute_select(self, mock_get_conn, mock_connection):
        """Test SELECT query execution."""
        from database.connection import execute_query

        conn, cursor = mock_connection
        mock_get_conn.return_value = conn

        # Mock fetchall to return sample data
        cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'BTCUSDT'},
            {'id': 2, 'symbol': 'ETHUSDT'}
        ]

        sql = "SELECT * FROM quality_metrics WHERE symbol = %(symbol)s"
        params = {'symbol': 'BTCUSDT'}

        results = execute_query(sql, params)

        assert len(results) == 2
        assert results[0]['symbol'] == 'BTCUSDT'

        # Verify RealDictCursor was used
        conn.cursor.assert_called_with(cursor_factory=RealDictCursor)

        # Verify execute was called with correct params
        cursor.execute.assert_called_once_with(sql, params)

    @patch('database.connection.get_db_connection')
    def test_execute_query_error(self, mock_get_conn, mock_connection):
        """Test query execution with error."""
        from database.connection import execute_query

        conn, cursor = mock_connection
        mock_get_conn.return_value = conn

        cursor.execute.side_effect = psycopg2.Error("Query failed")

        with pytest.raises(psycopg2.Error, match="Query failed"):
            execute_query("INVALID SQL")
