"""Tests for Prometheus quality metrics.

Tests coverage:
- Metric creation and labeling
- Update functions for all metric types
- Batch update from validator results
- Integration with QualityScorer, OutlierDetector, FreshnessMonitor, CompletenessValidator

Phase: AI Enhancement Plan Phase 5.6 - Metrics Integration
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / 'src'))

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Mock prometheus_client before importing metrics
sys.modules['prometheus_client'] = MagicMock()

from metrics.quality_metrics import (
    completeness_percentage,
    freshness_age_seconds,
    outlier_count,
    quality_score,
    record_quality_check_duration,
    update_completeness_metrics,
    update_freshness_metrics,
    update_metrics_from_quality_score,
    update_outlier_metrics,
    update_quality_metrics,
)


class TestQualityMetrics:
    """Test suite for quality metrics."""

    def test_update_quality_metrics(self):
        """Test updating quality metrics."""
        update_quality_metrics(
            symbol='BTCUSDT',
            quality_score_value=85.5,
            status='excellent',
            component_scores={
                'outlier': 90.0,
                'freshness': 85.0,
                'completeness': 95.0,
            },
            issues=[],
        )

        # Verify metric was called (labels method should be invoked)
        assert quality_score.labels.called

    def test_update_outlier_metrics(self):
        """Test updating outlier metrics."""
        update_outlier_metrics(
            symbol='ETHUSDT',
            field='close',
            outlier_count_value=5,
            severity='medium',
        )

        # Verify counter was incremented
        outlier_count.labels.assert_called_with(
            symbol='ETHUSDT',
            field='close',
            severity='medium',
        )

    def test_update_freshness_metrics(self):
        """Test updating freshness metrics."""
        update_freshness_metrics(
            symbol='BNBUSDT',
            age_seconds=120.5,
            status='warning',
        )

        # Verify gauge was set
        freshness_age_seconds.labels.assert_called_with(symbol='BNBUSDT', status='warning')

    def test_update_completeness_metrics(self):
        """Test updating completeness metrics."""
        update_completeness_metrics(
            symbol='ADAUSDT',
            completeness_pct=98.5,
            status='excellent',
        )

        # Verify gauge was set
        completeness_percentage.labels.assert_called_with(symbol='ADAUSDT', status='excellent')

    def test_record_quality_check_duration(self):
        """Test recording quality check duration."""
        record_quality_check_duration(
            symbol='BTCUSDT',
            duration_seconds=0.35,
        )

        # This test verifies the function runs without error
        # Actual histogram observation is tested in integration tests

    def test_update_with_issues(self):
        """Test metric updates with quality issues."""
        issues = [
            "Outliers in close: 5 (medium severity)",
            "Stale data: 10.5 minutes old",
            "Missing volume: 3 values",
        ]

        update_quality_metrics(
            symbol='BTCUSDT',
            quality_score_value=65.0,
            status='fair',
            component_scores={'outlier': 70.0, 'freshness': 60.0, 'completeness': 95.0},
            issues=issues,
        )

        # Verify quality score was set
        quality_score.labels.assert_called()

    def test_update_metrics_from_quality_score_object(self):
        """Test batch update from QualityScore object."""
        # Create mock QualityScore object
        mock_score = MagicMock()
        mock_score.symbol = 'BTCUSDT'
        mock_score.overall_score = 88.0
        mock_score.status = 'excellent'
        mock_score.component_scores = {
            'outlier': 90.0,
            'freshness': 85.0,
            'completeness': 95.0,
        }
        mock_score.issues = []

        update_metrics_from_quality_score(mock_score)

        # Verify metric was updated (labels should be called)
        assert quality_score.labels.called


class TestMetricIntegration:
    """Integration tests with validator results."""

    def test_outlier_result_integration(self):
        """Test integration with OutlierResult."""
        from validators.outlier_detector import OutlierResult

        result = OutlierResult(
            field='close',
            outlier_indices=[10, 20, 30],
            outlier_count=3,
            method='zscore',
            threshold=3.0,
            severity='low',
        )

        update_outlier_metrics(
            symbol='BTCUSDT',
            field=result.field,
            outlier_count_value=result.outlier_count,
            severity=result.severity,
        )

        outlier_count.labels.assert_called_with(
            symbol='BTCUSDT',
            field='close',
            severity='low',
        )

    def test_freshness_result_integration(self):
        """Test integration with FreshnessResult."""
        from validators.freshness_monitor import FreshnessResult

        result = FreshnessResult(
            symbol='ETHUSDT',
            last_timestamp=datetime.now(),
            age_seconds=90.0,
            age_minutes=1.5,
            status='fresh',
            gaps_detected=0,
            expected_frequency='1m',
        )

        update_freshness_metrics(
            symbol=result.symbol,
            age_seconds=result.age_seconds,
            status=result.status,
        )

        freshness_age_seconds.labels.assert_called_with(symbol='ETHUSDT', status='fresh')

    def test_completeness_result_integration(self):
        """Test integration with CompletenessResult."""
        from validators.completeness_validator import CompletenessResult

        result = CompletenessResult(
            symbol='BNBUSDT',
            total_rows=100,
            complete_rows=99,
            completeness_pct=99.0,
            missing_fields={},
            gaps_detected=0,
            min_points_met=True,
            status='excellent',
        )

        update_completeness_metrics(
            symbol=result.symbol,
            completeness_pct=result.completeness_pct,
            status=result.status,
        )

        completeness_percentage.labels.assert_called_with(symbol='BNBUSDT', status='excellent')


class TestMetricLabels:
    """Test metric labeling and multi-symbol support."""

    def test_multi_symbol_quality_scores(self):
        """Test quality scores for multiple symbols."""
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

        for symbol in symbols:
            update_quality_metrics(
                symbol=symbol,
                quality_score_value=80.0,
                status='good',
                component_scores={},
                issues=[],
            )

        # Verify all symbols were updated
        assert quality_score.labels.call_count >= 3

    def test_severity_labels(self):
        """Test outlier metrics with different severity levels."""
        severities = ['low', 'medium', 'high']

        for severity in severities:
            update_outlier_metrics(
                symbol='BTCUSDT',
                field='close',
                outlier_count_value=5,
                severity=severity,
            )

        # Verify all severities were tracked
        assert outlier_count.labels.call_count >= 3

    def test_status_labels(self):
        """Test freshness metrics with different statuses."""
        statuses = ['fresh', 'warning', 'critical']

        for status in statuses:
            update_freshness_metrics(
                symbol='BTCUSDT',
                age_seconds=60.0,
                status=status,
            )

        # Verify all statuses were tracked
        assert freshness_age_seconds.labels.call_count >= 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
