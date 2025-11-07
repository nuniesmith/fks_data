"""
Unit tests for QualityCollector

Tests the metrics collector wrapper that integrates quality scoring with Prometheus metrics.

Note: This test uses mocks for all dependencies. Full integration testing will be done
in Phase 5.6 Task 3 (Pipeline Integration).
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / 'src'))

# Mock prometheus_client before importing metrics
mock_gauge = Mock()
mock_counter = Mock()
mock_histogram = Mock()
mock_gauge.labels = Mock(return_value=mock_gauge)
mock_counter.labels = Mock(return_value=mock_counter)
mock_histogram.observe = Mock()

sys.modules['prometheus_client'] = Mock(
    Gauge=Mock(return_value=mock_gauge),
    Counter=Mock(return_value=mock_counter),
    Histogram=Mock(return_value=mock_histogram)
)

# Mock validators module before importing collector
sys.modules['validators'] = Mock()
sys.modules['validators.models'] = Mock()
sys.modules['validators.quality_scorer'] = Mock()
sys.modules['validators.outlier_detector'] = Mock()
sys.modules['validators.freshness_monitor'] = Mock()
sys.modules['validators.completeness_validator'] = Mock()

# Create mock classes for validators
class MockQualityScore:
    def __init__(self, score, status, symbol='BTCUSDT', issues=None, outlier_result=None,
                 freshness_result=None, completeness_result=None):
        self.score = score
        self.overall_score = score  # Alias for metrics compatibility
        self.status = status
        self.symbol = symbol
        self.issues = issues or []
        self.component_scores = {'outlier': 85.0, 'freshness': 90.0, 'completeness': 95.0}
        self.outlier_result = outlier_result
        self.freshness_result = freshness_result
        self.completeness_result = completeness_result

class MockOutlierResult:
    def __init__(self, has_outliers, outlier_fields, severity, outlier_count=0, details=None):
        self.has_outliers = has_outliers
        self.outlier_fields = outlier_fields
        self.severity = severity
        self.outlier_count = outlier_count
        self.details = details or {}

class MockFreshnessResult:
    def __init__(self, is_fresh, age_seconds, severity, timestamp):
        self.is_fresh = is_fresh
        self.age_seconds = age_seconds
        self.severity = severity
        self.timestamp = timestamp

class MockCompletenessResult:
    def __init__(self, is_complete, missing_fields, completeness_percentage, severity):
        self.is_complete = is_complete
        self.missing_fields = missing_fields
        self.completeness_percentage = completeness_percentage
        self.severity = severity

class MockSeverity:
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'

class MockQualityStatus:
    EXCELLENT = 'excellent'
    GOOD = 'good'
    FAIR = 'fair'
    POOR = 'poor'

# Set mock exports
sys.modules['validators.models'].QualityScore = MockQualityScore
sys.modules['validators.models'].OutlierResult = MockOutlierResult
sys.modules['validators.models'].FreshnessResult = MockFreshnessResult
sys.modules['validators.models'].CompletenessResult = MockCompletenessResult
sys.modules['validators.models'].Severity = MockSeverity
sys.modules['validators.models'].QualityStatus = MockQualityStatus

from metrics.quality_collector import QualityCollector, create_quality_collector


class TestQualityCollector:
    """Test QualityCollector basic functionality"""

    @patch('metrics.quality_collector.QualityScorer')
    def test_initialization(self, mock_scorer_class):
        """Test collector initialization with default settings"""
        collector = QualityCollector()

        assert collector.enable_metrics is True
        assert collector.enable_storage is False
        assert mock_scorer_class.called

    @patch('metrics.quality_collector.QualityScorer')
    @patch('metrics.quality_collector.OutlierDetector')
    @patch('metrics.quality_collector.FreshnessMonitor')
    @patch('metrics.quality_collector.CompletenessValidator')
    def test_initialization_custom_thresholds(
        self, mock_completeness_class, mock_freshness_class,
        mock_outlier_class, mock_scorer_class
    ):
        """Test collector initialization with custom thresholds"""
        # Setup mock detectors with expected attributes
        mock_outlier = Mock()
        mock_outlier.z_threshold = 2.5
        mock_outlier_class.return_value = mock_outlier

        mock_freshness = Mock()
        mock_freshness.max_age = timedelta(minutes=30)
        mock_freshness_class.return_value = mock_freshness

        mock_completeness = Mock()
        mock_completeness.threshold = 0.95
        mock_completeness_class.return_value = mock_completeness

        collector = QualityCollector(
            outlier_threshold=2.5,
            freshness_threshold=timedelta(minutes=30),
            completeness_threshold=0.95
        )

        assert collector.outlier_detector.z_threshold == 2.5
        assert collector.freshness_monitor.max_age == timedelta(minutes=30)
        assert collector.completeness_validator.threshold == 0.95

    @patch('metrics.quality_collector.QualityScorer')
    @patch('metrics.quality_collector.update_metrics_from_quality_score')
    @patch('metrics.quality_collector.record_quality_check_duration')
    def test_check_quality_updates_metrics(
        self, mock_record_duration, mock_update_metrics, mock_scorer_class
    ):
        """Test that check_quality updates Prometheus metrics"""
        # Setup
        collector = QualityCollector(enable_metrics=True)
        mock_scorer = Mock()
        collector.quality_scorer = mock_scorer

        quality_score = MockQualityScore(
            score=85.0,
            status=MockQualityStatus.GOOD,
            symbol='BTCUSDT',
            issues=[],
            outlier_result=None,
            freshness_result=None,
            completeness_result=None
        )
        mock_scorer.check_quality.return_value = quality_score

        # Execute
        data = {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000.0}
        result = collector.check_quality('BTCUSDT', data)

        # Verify
        assert result == quality_score
        assert mock_update_metrics.called
        assert mock_record_duration.called

        # Check that duration was recorded
        duration_call = mock_record_duration.call_args
        assert duration_call[0][0] == 'BTCUSDT'
        assert isinstance(duration_call[0][1], float)

    @patch('metrics.quality_collector.QualityScorer')
    def test_check_quality_no_metrics(self, mock_scorer_class):
        """Test that metrics are not updated when disabled"""
        collector = QualityCollector(enable_metrics=False)
        mock_scorer = Mock()
        collector.quality_scorer = mock_scorer

        quality_score = MockQualityScore(
            score=85.0,
            status=MockQualityStatus.GOOD,
            symbol='BTCUSDT',
            issues=[],
            outlier_result=None,
            freshness_result=None,
            completeness_result=None
        )
        mock_scorer.check_quality.return_value = quality_score

        data = {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000.0}

        with patch('metrics.quality_collector.update_metrics_from_quality_score') as mock_update:
            collector.check_quality('BTCUSDT', data)
            assert not mock_update.called

    @pytest.mark.anyio
    @patch('metrics.quality_collector.QualityScorer')
    async def test_check_quality_batch(self, mock_scorer_class):
        """Test batch quality checking for multiple symbols"""
        collector = QualityCollector(enable_metrics=False)
        mock_scorer = Mock()
        collector.quality_scorer = mock_scorer

        # Setup responses
        btc_score = MockQualityScore(
            score=85.0, status=MockQualityStatus.GOOD, symbol='BTCUSDT', issues=[],
            outlier_result=None, freshness_result=None, completeness_result=None
        )
        eth_score = MockQualityScore(
            score=90.0, status=MockQualityStatus.EXCELLENT, symbol='ETHUSDT', issues=[],
            outlier_result=None, freshness_result=None, completeness_result=None
        )

        mock_scorer.check_quality.side_effect = [btc_score, eth_score]

        # Execute
        data_dict = {
            'BTCUSDT': {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000.0},
            'ETHUSDT': {'open': 200.0, 'high': 201.0, 'low': 199.0, 'close': 200.5, 'volume': 2000.0}
        }
        results = await collector.check_quality_batch(['BTCUSDT', 'ETHUSDT'], data_dict)

        # Verify
        assert len(results) == 2
        assert results['BTCUSDT'] == btc_score
        assert results['ETHUSDT'] == eth_score

    @pytest.mark.anyio
    @patch('metrics.quality_collector.QualityScorer')
    async def test_check_quality_batch_missing_data(self, mock_scorer_class):
        """Test batch checking handles missing data gracefully"""
        collector = QualityCollector(enable_metrics=False)
        mock_scorer = Mock()
        collector.quality_scorer = mock_scorer

        btc_score = MockQualityScore(
            score=85.0, status=MockQualityStatus.GOOD, symbol='BTCUSDT', issues=[],
            outlier_result=None, freshness_result=None, completeness_result=None
        )
        mock_scorer.check_quality.return_value = btc_score

        # Execute with missing symbol
        data_dict = {
            'BTCUSDT': {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000.0}
        }
        results = await collector.check_quality_batch(['BTCUSDT', 'ETHUSDT'], data_dict)

        # Verify - only BTCUSDT processed
        assert len(results) == 1
        assert 'BTCUSDT' in results
        assert 'ETHUSDT' not in results


class TestIndividualValidators:
    """Test individual validator methods"""

    @patch('metrics.quality_collector.OutlierDetector')
    @patch('metrics.quality_collector.update_outlier_metrics')
    def test_check_outliers(self, mock_update_metrics, mock_detector_class):
        """Test outlier checking with metrics update"""
        collector = QualityCollector(enable_metrics=True)

        outlier_result = MockOutlierResult(
            has_outliers=True,
            outlier_fields=['close'],
            severity=MockSeverity.MEDIUM,
            outlier_count=1,
            details={'close': {'z_score': 3.5, 'value': 150.0}}
        )

        mock_detector = Mock()
        mock_detector.detect_outliers.return_value = outlier_result
        collector.outlier_detector = mock_detector

        data = {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 150.0, 'volume': 1000.0}
        result = collector.check_outliers('BTCUSDT', data)

        assert result == outlier_result
        assert mock_update_metrics.called
        # Check that it was called for the 'close' field
        mock_update_metrics.assert_called_once()

    @patch('metrics.quality_collector.FreshnessMonitor')
    @patch('metrics.quality_collector.update_metrics_from_freshness_result')
    def test_check_freshness(self, mock_update_metrics, mock_monitor_class):
        """Test freshness checking with metrics update"""
        collector = QualityCollector(enable_metrics=True)

        freshness_result = MockFreshnessResult(
            is_fresh=False,
            age_seconds=1200.0,
            severity=MockSeverity.HIGH,
            timestamp=datetime(2025, 1, 1, 12, 0, 0)
        )

        mock_monitor = Mock()
        mock_monitor.check_freshness.return_value = freshness_result
        collector.freshness_monitor = mock_monitor

        timestamp = datetime(2025, 1, 1, 12, 0, 0)
        result = collector.check_freshness('BTCUSDT', timestamp)

        assert result == freshness_result
        assert mock_update_metrics.called

    @patch('metrics.quality_collector.CompletenessValidator')
    @patch('metrics.quality_collector.update_metrics_from_completeness_result')
    def test_check_completeness(self, mock_update_metrics, mock_validator_class):
        """Test completeness checking with metrics update"""
        collector = QualityCollector(enable_metrics=True)

        completeness_result = MockCompletenessResult(
            is_complete=True,
            missing_fields=[],
            completeness_percentage=100.0,
            severity=MockSeverity.LOW
        )

        mock_validator = Mock()
        mock_validator.validate_completeness.return_value = completeness_result
        collector.completeness_validator = mock_validator

        data = {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000.0}
        result = collector.check_completeness('BTCUSDT', data)

        assert result == completeness_result
        assert mock_update_metrics.called


class TestFactoryFunction:
    """Test factory function for creating collectors"""

    @patch('metrics.quality_collector.OutlierDetector')
    @patch('metrics.quality_collector.FreshnessMonitor')
    @patch('metrics.quality_collector.CompletenessValidator')
    def test_create_quality_collector_defaults(
        self, mock_completeness_class, mock_freshness_class, mock_outlier_class
    ):
        """Test factory function with default settings"""
        # Setup mock detectors
        mock_outlier = Mock()
        mock_outlier.z_threshold = 3.0
        mock_outlier_class.return_value = mock_outlier

        mock_freshness = Mock()
        mock_freshness.max_age = timedelta(minutes=15)
        mock_freshness_class.return_value = mock_freshness

        mock_completeness = Mock()
        mock_completeness.threshold = 0.9
        mock_completeness_class.return_value = mock_completeness

        collector = create_quality_collector()

        assert collector.enable_metrics is True
        assert collector.enable_storage is False
        assert collector.outlier_detector.z_threshold == 3.0
        assert collector.freshness_monitor.max_age == timedelta(minutes=15)
        assert collector.completeness_validator.threshold == 0.9

    @patch('metrics.quality_collector.OutlierDetector')
    @patch('metrics.quality_collector.FreshnessMonitor')
    @patch('metrics.quality_collector.CompletenessValidator')
    def test_create_quality_collector_custom(
        self, mock_completeness_class, mock_freshness_class, mock_outlier_class
    ):
        """Test factory function with custom settings"""
        # Setup mock detectors
        mock_outlier = Mock()
        mock_outlier.z_threshold = 2.5
        mock_outlier_class.return_value = mock_outlier

        mock_freshness = Mock()
        mock_freshness.max_age = timedelta(minutes=30)
        mock_freshness_class.return_value = mock_freshness

        mock_completeness = Mock()
        mock_completeness.threshold = 0.95
        mock_completeness_class.return_value = mock_completeness

        collector = create_quality_collector(
            outlier_threshold=2.5,
            freshness_minutes=30,
            completeness_threshold=0.95,
            enable_metrics=False,
            enable_storage=True
        )

        assert collector.enable_metrics is False
        assert collector.enable_storage is True
        assert collector.outlier_detector.z_threshold == 2.5
        assert collector.freshness_monitor.max_age == timedelta(minutes=30)
        assert collector.completeness_validator.threshold == 0.95


class TestStorageIntegration:
    """Test TimescaleDB storage integration (placeholder)"""

    @patch('metrics.quality_collector.QualityScorer')
    def test_storage_disabled_by_default(self, mock_scorer_class):
        """Test that storage is disabled by default"""
        collector = QualityCollector()
        assert collector.enable_storage is False

    @patch('metrics.quality_collector.QualityScorer')
    def test_storage_enabled(self, mock_scorer_class):
        """Test that storage can be enabled"""
        collector = QualityCollector(enable_storage=True)
        assert collector.enable_storage is True

        # Note: Actual storage implementation in Phase 5.6 Task 3
        mock_scorer = Mock()
        collector.quality_scorer = mock_scorer

        quality_score = MockQualityScore(
            score=85.0, status=MockQualityStatus.GOOD, symbol='BTCUSDT', issues=[],
            outlier_result=None, freshness_result=None, completeness_result=None
        )
        mock_scorer.check_quality.return_value = quality_score

        data = {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000.0}

        # Should not raise even though storage is not implemented yet
        result = collector.check_quality('BTCUSDT', data)
        assert result == quality_score
