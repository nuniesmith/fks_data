"""Tests for Phase 5.5 Data Quality Validators.

Tests coverage:
- OutlierDetector: zscore, iqr, mad methods, severity classification, cleaning
- FreshnessMonitor: staleness detection, gap detection, multi-symbol monitoring
- CompletenessValidator: missing fields, null values, minimum points
- QualityScorer: combined scoring, recommendations, multi-symbol scoring

Phase: AI Enhancement Plan Phase 5.5 - Data Quality Validation
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / 'src'))

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from validators.completeness_validator import CompletenessResult, CompletenessValidator
from validators.freshness_monitor import FreshnessMonitor, FreshnessResult
from validators.outlier_detector import OutlierDetector, OutlierResult
from validators.quality_scorer import QualityScore, QualityScorer

# ============================================================================
# OutlierDetector Tests
# ============================================================================

class TestOutlierDetector:
    """Test suite for OutlierDetector."""

    @pytest.fixture
    def clean_data(self):
        """Create clean data without outliers."""
        np.random.seed(42)
        return pd.DataFrame({
            'close': np.random.normal(100, 5, 100),
            'volume': np.random.normal(1000, 100, 100),
        })

    @pytest.fixture
    def data_with_outliers(self):
        """Create data with outliers."""
        np.random.seed(42)
        data = pd.DataFrame({
            'close': np.random.normal(100, 5, 100),
            'volume': np.random.normal(1000, 100, 100),
        })
        # Add outliers
        data.loc[10, 'close'] = 200  # +20 std
        data.loc[20, 'volume'] = 5000  # +40 std
        data.loc[30, 'close'] = 50  # -10 std
        return data

    def test_initialization(self):
        """Test detector initialization."""
        detector = OutlierDetector(method='zscore', threshold=3.0)
        assert detector.method == 'zscore'
        assert detector.threshold == 3.0

    def test_zscore_clean_data(self, clean_data):
        """Test zscore on clean data."""
        detector = OutlierDetector(method='zscore', threshold=3.0)
        results = detector.detect(clean_data, fields=['close'])

        assert len(results) == 1
        assert results[0].field == 'close'
        assert results[0].outlier_count == 0  # No outliers
        assert results[0].severity == 'low'

    def test_zscore_with_outliers(self, data_with_outliers):
        """Test zscore detection with outliers."""
        detector = OutlierDetector(method='zscore', threshold=3.0)
        results = detector.detect(data_with_outliers, fields=['close'])

        assert len(results) == 1
        assert results[0].outlier_count > 0  # Outliers detected
        assert results[0].method == 'zscore'

    def test_iqr_detection(self, data_with_outliers):
        """Test IQR outlier detection."""
        detector = OutlierDetector(method='iqr', threshold=1.5)
        results = detector.detect(data_with_outliers, fields=['volume'])

        assert len(results) == 1
        assert results[0].method == 'iqr'
        assert results[0].outlier_count > 0

    def test_mad_detection(self, data_with_outliers):
        """Test MAD outlier detection."""
        detector = OutlierDetector(method='mad', threshold=3.0)
        results = detector.detect(data_with_outliers, fields=['close', 'volume'])

        assert len(results) == 2  # Both fields
        for result in results:
            assert result.method == 'mad'

    def test_severity_classification(self, data_with_outliers):
        """Test severity classification."""
        detector = OutlierDetector(method='zscore', threshold=3.0)
        results = detector.detect(data_with_outliers, fields=['close'])

        result = results[0]
        outlier_pct = (result.outlier_count / len(data_with_outliers)) * 100

        if outlier_pct < 5:
            assert result.severity == 'low'
        elif outlier_pct < 10:
            assert result.severity == 'medium'
        else:
            assert result.severity == 'high'

    def test_clean_outliers_remove(self, data_with_outliers):
        """Test outlier removal."""
        detector = OutlierDetector(method='zscore', threshold=3.0)
        results = detector.detect(data_with_outliers, fields=['close'])

        cleaned = detector.clean_outliers(
            data_with_outliers.copy(),
            results,
            method='remove',
        )

        assert len(cleaned) < len(data_with_outliers)

    def test_clean_outliers_interpolate(self, data_with_outliers):
        """Test outlier interpolation."""
        detector = OutlierDetector(method='zscore', threshold=3.0)
        results = detector.detect(data_with_outliers, fields=['close'])

        cleaned = detector.clean_outliers(
            data_with_outliers.copy(),
            results,
            method='interpolate',
        )

        # Should have same length
        assert len(cleaned) == len(data_with_outliers)
        # Outliers should be replaced
        assert not cleaned['close'].isna().any()

    def test_empty_data(self):
        """Test with empty DataFrame."""
        detector = OutlierDetector()
        results = detector.detect(pd.DataFrame(), fields=['close'])

        assert len(results) == 0


# ============================================================================
# FreshnessMonitor Tests
# ============================================================================

class TestFreshnessMonitor:
    """Test suite for FreshnessMonitor."""

    @pytest.fixture
    def fresh_data(self):
        """Create fresh data (last timestamp within 1 minute)."""
        now = datetime.now()
        timestamps = [now - timedelta(minutes=i) for i in range(10)]
        return pd.DataFrame({
            'timestamp': timestamps,
            'close': np.random.normal(100, 5, 10),
        })

    @pytest.fixture
    def stale_data(self):
        """Create stale data (last timestamp 20 minutes old)."""
        now = datetime.now()
        start_time = now - timedelta(minutes=30)
        timestamps = [start_time + timedelta(minutes=i) for i in range(10)]
        return pd.DataFrame({
            'timestamp': timestamps,
            'close': np.random.normal(100, 5, 10),
        })

    def test_initialization(self):
        """Test monitor initialization."""
        monitor = FreshnessMonitor(warning_threshold=5, critical_threshold=15)
        assert monitor.warning_threshold == 5
        assert monitor.critical_threshold == 15

    def test_fresh_data_status(self, fresh_data):
        """Test fresh data detection."""
        monitor = FreshnessMonitor()
        result = monitor.check_freshness(fresh_data, symbol='BTCUSDT', frequency='1m')

        assert result.status == 'fresh'
        assert not result.is_stale
        assert result.age_minutes < 5

    def test_stale_data_status(self, stale_data):
        """Test stale data detection."""
        monitor = FreshnessMonitor()
        result = monitor.check_freshness(stale_data, symbol='BTCUSDT', frequency='1m')

        assert result.status == 'critical'
        assert result.is_stale
        assert result.age_minutes > 15

    def test_warning_threshold(self):
        """Test warning threshold."""
        monitor = FreshnessMonitor(warning_threshold=5, critical_threshold=15)

        # Create data 7 minutes old
        now = datetime.now()
        start_time = now - timedelta(minutes=17)
        timestamps = [start_time + timedelta(minutes=i) for i in range(10)]
        data = pd.DataFrame({'timestamp': timestamps, 'close': [100]*10})

        result = monitor.check_freshness(data, symbol='TEST', frequency='1m')

        # Should be in warning or critical range
        assert result.status in ['warning', 'critical']
        assert result.needs_refresh

    def test_gap_detection(self):
        """Test time-series gap detection."""
        monitor = FreshnessMonitor()

        # Create data with gaps
        now = datetime.now()
        timestamps = [
            now - timedelta(minutes=10),
            now - timedelta(minutes=9),
            now - timedelta(minutes=8),
            now - timedelta(minutes=4),  # 4-minute gap
            now - timedelta(minutes=3),
            now - timedelta(minutes=2),
        ]
        data = pd.DataFrame({'timestamp': timestamps, 'close': [100]*6})

        result = monitor.check_freshness(data, symbol='BTCUSDT', frequency='1m')

        assert result.gaps_detected > 0

    def test_multiple_symbols(self, fresh_data, stale_data):
        """Test monitoring multiple symbols."""
        monitor = FreshnessMonitor()

        data_dict = {
            'BTCUSDT': fresh_data,
            'ETHUSDT': stale_data,
        }

        results = monitor.check_multiple(data_dict, frequency='1m')

        assert len(results) == 2
        assert results['BTCUSDT'].status == 'fresh'
        assert results['ETHUSDT'].status == 'critical'

    def test_empty_data(self):
        """Test with empty DataFrame."""
        monitor = FreshnessMonitor()
        result = monitor.check_freshness(pd.DataFrame(), symbol='TEST', frequency='1m')

        assert result.status == 'critical'
        assert result.age_minutes == float('inf')


# ============================================================================
# CompletenessValidator Tests
# ============================================================================

class TestCompletenessValidator:
    """Test suite for CompletenessValidator."""

    @pytest.fixture
    def complete_data(self):
        """Create complete OHLCV data."""
        return pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=100, freq='1min'),
            'open': np.random.normal(100, 5, 100),
            'high': np.random.normal(105, 5, 100),
            'low': np.random.normal(95, 5, 100),
            'close': np.random.normal(100, 5, 100),
            'volume': np.random.normal(1000, 100, 100),
        })

    @pytest.fixture
    def incomplete_data(self):
        """Create incomplete data with missing values."""
        data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=100, freq='1min'),
            'open': np.random.normal(100, 5, 100),
            'high': np.random.normal(105, 5, 100),
            'low': np.random.normal(95, 5, 100),
            'close': np.random.normal(100, 5, 100),
            'volume': np.random.normal(1000, 100, 100),
        })
        # Add missing values
        data.loc[10:20, 'close'] = np.nan
        data.loc[30:35, 'volume'] = np.nan
        return data

    def test_initialization(self):
        """Test validator initialization."""
        validator = CompletenessValidator()
        assert 'open' in validator.required_fields
        assert 'close' in validator.required_fields

    def test_complete_data_validation(self, complete_data):
        """Test validation of complete data."""
        validator = CompletenessValidator()
        result = validator.validate(complete_data, symbol='BTCUSDT', min_points=50)

        assert result.status == 'excellent'
        assert result.is_complete
        assert result.completeness_pct >= 99.0
        assert result.min_points_met

    def test_incomplete_data_validation(self, incomplete_data):
        """Test validation of incomplete data."""
        validator = CompletenessValidator()
        result = validator.validate(incomplete_data, symbol='BTCUSDT', min_points=50)

        assert result.completeness_pct < 100.0
        assert result.has_critical_gaps
        assert sum(result.missing_fields.values()) > 0

    def test_missing_required_fields(self):
        """Test detection of missing required fields."""
        data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=100, freq='1min'),
            'close': np.random.normal(100, 5, 100),
            # Missing: open, high, low, volume
        })

        validator = CompletenessValidator()
        result = validator.validate(data, symbol='TEST', min_points=50)

        assert result.status == 'poor'
        assert not result.is_complete

    def test_minimum_points_check(self, complete_data):
        """Test minimum points requirement."""
        validator = CompletenessValidator()

        # Should pass with min_points=50
        result1 = validator.validate(complete_data, min_points=50)
        assert result1.min_points_met

        # Should fail with min_points=200
        result2 = validator.validate(complete_data, min_points=200)
        assert not result2.min_points_met

    def test_multiple_symbols(self, complete_data, incomplete_data):
        """Test validation of multiple symbols."""
        validator = CompletenessValidator()

        data_dict = {
            'BTCUSDT': complete_data,
            'ETHUSDT': incomplete_data,
        }

        results = validator.validate_multiple(data_dict, min_points=50)

        assert len(results) == 2
        assert results['BTCUSDT'].status == 'excellent'
        assert results['ETHUSDT'].has_critical_gaps

    def test_empty_data(self):
        """Test with empty DataFrame."""
        validator = CompletenessValidator()
        result = validator.validate(pd.DataFrame(), symbol='TEST', min_points=50)

        assert result.status == 'poor'
        assert result.completeness_pct == 0.0


# ============================================================================
# QualityScorer Tests
# ============================================================================

class TestQualityScorer:
    """Test suite for QualityScorer."""

    @pytest.fixture
    def excellent_data(self):
        """Create excellent quality data."""
        return pd.DataFrame({
            'timestamp': pd.date_range(datetime.now() - timedelta(minutes=10), periods=100, freq='1min'),
            'open': np.random.normal(100, 2, 100),  # Low variance, no outliers
            'high': np.random.normal(105, 2, 100),
            'low': np.random.normal(95, 2, 100),
            'close': np.random.normal(100, 2, 100),
            'volume': np.random.normal(1000, 50, 100),
        })

    @pytest.fixture
    def poor_data(self):
        """Create poor quality data."""
        data = pd.DataFrame({
            'timestamp': pd.date_range(datetime.now() - timedelta(minutes=30), periods=50, freq='1min'),
            'open': np.random.normal(100, 5, 50),
            'high': np.random.normal(105, 5, 50),
            'low': np.random.normal(95, 5, 50),
            'close': np.random.normal(100, 5, 50),
            'volume': np.random.normal(1000, 100, 50),
        })
        # Add outliers
        data.loc[10, 'close'] = 500
        data.loc[20, 'volume'] = 10000
        # Add missing values
        data.loc[30:35, 'close'] = np.nan
        return data

    def test_initialization(self):
        """Test scorer initialization."""
        scorer = QualityScorer()
        assert scorer.outlier_weight + scorer.freshness_weight + scorer.completeness_weight == 1.0

    def test_weight_validation(self):
        """Test weight sum validation."""
        with pytest.raises(ValueError):
            QualityScorer(outlier_weight=0.5, freshness_weight=0.5, completeness_weight=0.5)

    def test_excellent_quality_score(self, excellent_data):
        """Test scoring of excellent quality data."""
        scorer = QualityScorer()
        score = scorer.score(excellent_data, symbol='BTCUSDT', frequency='1m', min_points=50)

        assert score.overall_score >= 70.0  # Should be good or excellent
        assert score.is_good_quality
        assert score.status in ['good', 'excellent']

    def test_poor_quality_score(self, poor_data):
        """Test scoring of poor quality data."""
        scorer = QualityScorer()
        score = scorer.score(poor_data, symbol='BTCUSDT', frequency='1m', min_points=50)

        # Should have lower score due to outliers, staleness, missing values
        assert len(score.issues) > 0
        assert len(score.recommendations) > 0

    def test_component_scores(self, excellent_data):
        """Test individual component scores."""
        scorer = QualityScorer()
        score = scorer.score(excellent_data, symbol='BTCUSDT', frequency='1m', min_points=50)

        assert 'outlier' in score.component_scores
        assert 'freshness' in score.component_scores
        assert 'completeness' in score.component_scores

        # All components should contribute
        for _component, comp_score in score.component_scores.items():
            assert 0 <= comp_score <= 100

    def test_recommendations_generated(self, poor_data):
        """Test that recommendations are generated for poor data."""
        scorer = QualityScorer()
        score = scorer.score(poor_data, symbol='BTCUSDT', frequency='1m', min_points=50)

        assert len(score.recommendations) > 0
        # Should have specific actionable recommendations
        recommendations_text = ' '.join(score.recommendations)
        assert any(keyword in recommendations_text.lower() for keyword in ['clean', 'refresh', 'fill'])

    def test_multiple_symbols(self, excellent_data, poor_data):
        """Test scoring multiple symbols."""
        scorer = QualityScorer()

        data_dict = {
            'BTCUSDT': excellent_data,
            'ETHUSDT': poor_data,
        }

        scores = scorer.score_multiple(data_dict, frequency='1m', min_points=50)

        assert len(scores) == 2
        assert scores['BTCUSDT'].overall_score > scores['ETHUSDT'].overall_score

    def test_quality_summary(self, excellent_data, poor_data):
        """Test quality summary generation."""
        scorer = QualityScorer()

        scores = {
            'BTCUSDT': scorer.score(excellent_data, symbol='BTCUSDT', frequency='1m'),
            'ETHUSDT': scorer.score(poor_data, symbol='ETHUSDT', frequency='1m'),
        }

        summary = scorer.get_quality_summary(scores)

        assert summary['total_symbols'] == 2
        assert 'avg_quality_score' in summary
        assert 'min_quality_score' in summary

    def test_empty_data(self):
        """Test scoring empty data."""
        scorer = QualityScorer()
        score = scorer.score(pd.DataFrame(), symbol='TEST', frequency='1m')

        assert score.overall_score == 0.0
        assert score.status == 'poor'
        assert len(score.issues) > 0


# ============================================================================
# Integration Tests
# ============================================================================

class TestValidatorIntegration:
    """Integration tests combining multiple validators."""

    def test_full_quality_pipeline(self):
        """Test complete quality assessment pipeline."""
        # Create test data
        data = pd.DataFrame({
            'timestamp': pd.date_range(datetime.now() - timedelta(minutes=10), periods=100, freq='1min'),
            'open': np.random.normal(100, 3, 100),
            'high': np.random.normal(105, 3, 100),
            'low': np.random.normal(95, 3, 100),
            'close': np.random.normal(100, 3, 100),
            'volume': np.random.normal(1000, 100, 100),
        })

        # Run all validators
        outlier_detector = OutlierDetector()
        freshness_monitor = FreshnessMonitor()
        completeness_validator = CompletenessValidator()
        quality_scorer = QualityScorer()

        # Individual validations
        outliers = outlier_detector.detect(data, fields=['close', 'volume'])
        freshness = freshness_monitor.check_freshness(data, symbol='BTCUSDT', frequency='1m')
        completeness = completeness_validator.validate(data, symbol='BTCUSDT', min_points=50)

        # Combined score
        score = quality_scorer.score(data, symbol='BTCUSDT', frequency='1m', min_points=50)

        # Verify all ran successfully
        assert len(outliers) == 2
        assert freshness.symbol == 'BTCUSDT'
        assert completeness.symbol == 'BTCUSDT'
        assert score.symbol == 'BTCUSDT'
        assert score.overall_score > 0

    def test_multi_symbol_quality_assessment(self):
        """Test quality assessment across multiple symbols."""
        # Create multiple symbol datasets
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        data_dict = {}

        for symbol in symbols:
            data_dict[symbol] = pd.DataFrame({
                'timestamp': pd.date_range(datetime.now() - timedelta(minutes=10), periods=100, freq='1min'),
                'open': np.random.normal(100, 3, 100),
                'high': np.random.normal(105, 3, 100),
                'low': np.random.normal(95, 3, 100),
                'close': np.random.normal(100, 3, 100),
                'volume': np.random.normal(1000, 100, 100),
            })

        # Score all symbols
        scorer = QualityScorer()
        scores = scorer.score_multiple(data_dict, frequency='1m', min_points=50)

        # Verify all symbols scored
        assert len(scores) == 3
        for symbol in symbols:
            assert symbol in scores
            assert scores[symbol].overall_score > 0

        # Verify summary
        summary = scorer.get_quality_summary(scores)
        assert summary['total_symbols'] == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
