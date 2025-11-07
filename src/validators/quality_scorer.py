"""Data quality scoring system combining multiple validators.

This module:
- Combines outlier, freshness, and completeness metrics
- Generates 0-100 quality score
- Provides actionable recommendations
- Supports dashboard visualization

Quality Score Thresholds:
- Excellent: 85-100
- Good: 70-85
- Fair: 50-70
- Poor: <50

Phase: AI Enhancement Plan Phase 5.5 - Data Quality Validation
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .completeness_validator import CompletenessResult, CompletenessValidator
from .freshness_monitor import FreshnessMonitor, FreshnessResult
from .outlier_detector import OutlierDetector, OutlierResult

logger = logging.getLogger(__name__)


@dataclass
class QualityScore:
    """Overall data quality score.

    Attributes:
        symbol: Trading symbol
        overall_score: 0-100 quality score
        component_scores: Dictionary of component -> score
        status: 'excellent', 'good', 'fair', 'poor'
        issues: List of identified issues
        recommendations: List of recommended actions
        timestamp: When score was calculated
    """
    symbol: str
    overall_score: float
    component_scores: dict[str, float]
    status: str
    issues: list[str]
    recommendations: list[str]
    timestamp: datetime

    @property
    def is_acceptable(self) -> bool:
        """Check if quality meets minimum threshold (>50)."""
        return self.overall_score >= 50.0

    @property
    def is_good_quality(self) -> bool:
        """Check if quality is good (>70)."""
        return self.overall_score >= 70.0


class QualityScorer:
    """Generate overall data quality scores.

    Combines multiple validators to produce a comprehensive quality assessment.
    Provides actionable recommendations for quality improvement.

    Example:
        >>> scorer = QualityScorer()
        >>> score = scorer.score(df, symbol='BTCUSDT', frequency='1m')
        >>> print(f"Quality: {score.overall_score:.1f}/100 ({score.status})")
        >>> for issue in score.issues:
        ...     print(f"  - {issue}")
    """

    def __init__(
        self,
        outlier_weight: float = 0.3,
        freshness_weight: float = 0.3,
        completeness_weight: float = 0.4,
        excellent_threshold: float = 85.0,
        good_threshold: float = 70.0,
        fair_threshold: float = 50.0,
    ):
        """Initialize quality scorer.

        Args:
            outlier_weight: Weight for outlier score (default 0.3)
            freshness_weight: Weight for freshness score (default 0.3)
            completeness_weight: Weight for completeness score (default 0.4)
            excellent_threshold: Threshold for 'excellent' (default 85)
            good_threshold: Threshold for 'good' (default 70)
            fair_threshold: Threshold for 'fair' (default 50)
        """
        # Validate weights sum to 1.0
        total_weight = outlier_weight + freshness_weight + completeness_weight
        if not np.isclose(total_weight, 1.0):
            raise ValueError(f"Weights must sum to 1.0, got {total_weight}")

        self.outlier_weight = outlier_weight
        self.freshness_weight = freshness_weight
        self.completeness_weight = completeness_weight

        self.excellent_threshold = excellent_threshold
        self.good_threshold = good_threshold
        self.fair_threshold = fair_threshold

        # Initialize validators
        self.outlier_detector = OutlierDetector()
        self.freshness_monitor = FreshnessMonitor()
        self.completeness_validator = CompletenessValidator()

        logger.info(
            f"QualityScorer initialized "
            f"(weights: outlier={outlier_weight}, freshness={freshness_weight}, "
            f"completeness={completeness_weight})"
        )

    def score(
        self,
        data: pd.DataFrame,
        symbol: str = "",
        frequency: str = '1m',
        timestamp_col: str = 'timestamp',
        min_points: int = 50,
    ) -> QualityScore:
        """Calculate overall quality score for data.

        Args:
            data: DataFrame to score
            symbol: Trading symbol
            frequency: Data frequency
            timestamp_col: Timestamp column name
            min_points: Minimum required data points

        Returns:
            QualityScore object
        """
        if data.empty:
            logger.warning(f"Empty data for symbol: {symbol}")
            return self._create_poor_score(symbol, "No data available")

        # Run all validators
        outlier_results = self.outlier_detector.detect(
            data,
            fields=['close', 'volume'],
        )

        freshness_result = self.freshness_monitor.check_freshness(
            data,
            symbol=symbol,
            frequency=frequency,
            timestamp_col=timestamp_col,
        )

        completeness_result = self.completeness_validator.validate(
            data,
            symbol=symbol,
            min_points=min_points,
            timestamp_col=timestamp_col,
            expected_frequency=frequency,
        )

        # Calculate component scores
        outlier_score = self._score_outliers(outlier_results, len(data))
        freshness_score = self._score_freshness(freshness_result)
        completeness_score = self._score_completeness(completeness_result)

        # Calculate weighted overall score
        overall_score = (
            outlier_score * self.outlier_weight +
            freshness_score * self.freshness_weight +
            completeness_score * self.completeness_weight
        )

        # Determine status
        status = self._classify_score(overall_score)

        # Identify issues
        issues = self._identify_issues(
            outlier_results,
            freshness_result,
            completeness_result,
        )

        # Generate recommendations
        recommendations = self._generate_recommendations(
            issues,
            outlier_score,
            freshness_score,
            completeness_score,
        )

        score = QualityScore(
            symbol=symbol,
            overall_score=overall_score,
            component_scores={
                'outlier': outlier_score,
                'freshness': freshness_score,
                'completeness': completeness_score,
            },
            status=status,
            issues=issues,
            recommendations=recommendations,
            timestamp=datetime.now(),
        )

        logger.info(
            f"Quality score for {symbol}: {overall_score:.1f}/100 ({status}) - "
            f"outlier={outlier_score:.1f}, freshness={freshness_score:.1f}, "
            f"completeness={completeness_score:.1f}"
        )

        return score

    def score_multiple(
        self,
        data_dict: dict[str, pd.DataFrame],
        frequency: str = '1m',
        timestamp_col: str = 'timestamp',
        min_points: int = 50,
    ) -> dict[str, QualityScore]:
        """Score multiple symbols.

        Args:
            data_dict: Dictionary mapping symbols to DataFrames
            frequency: Data frequency
            timestamp_col: Timestamp column name
            min_points: Minimum required data points

        Returns:
            Dictionary mapping symbols to QualityScore objects
        """
        scores = {}

        for symbol, data in data_dict.items():
            scores[symbol] = self.score(
                data,
                symbol=symbol,
                frequency=frequency,
                timestamp_col=timestamp_col,
                min_points=min_points,
            )

        return scores

    def _score_outliers(
        self,
        outlier_results: list[OutlierResult],
        total_rows: int,
    ) -> float:
        """Convert outlier results to 0-100 score.

        Args:
            outlier_results: List of OutlierResult objects
            total_rows: Total number of rows

        Returns:
            Score 0-100 (100 = no outliers)
        """
        if total_rows == 0:
            return 0.0

        # Count total outliers
        total_outliers = sum(r.outlier_count for r in outlier_results)

        # Calculate outlier percentage
        outlier_pct = (total_outliers / total_rows) * 100

        # Convert to score (0% outliers = 100, >10% = 0)
        score = max(0, 100 - (outlier_pct * 10))

        return float(score)

    def _score_freshness(self, freshness_result: FreshnessResult) -> float:
        """Convert freshness result to 0-100 score.

        Args:
            freshness_result: FreshnessResult object

        Returns:
            Score 0-100 (100 = fresh data)
        """
        if freshness_result.status == 'fresh':
            return 100.0
        elif freshness_result.status == 'warning':
            # Linear decay from 100 to 50 based on age
            age_minutes = min(freshness_result.age_minutes, 15)
            score = 100 - ((age_minutes - 1) / 14) * 50
            return float(max(50, score))
        else:  # critical
            # Linear decay from 50 to 0 based on age
            age_minutes = min(freshness_result.age_minutes, 60)
            score = 50 - ((age_minutes - 15) / 45) * 50
            return float(max(0, score))

    def _score_completeness(
        self,
        completeness_result: CompletenessResult,
    ) -> float:
        """Convert completeness result to 0-100 score.

        Args:
            completeness_result: CompletenessResult object

        Returns:
            Score 0-100 (100 = complete data)
        """
        # Use completeness percentage directly
        score = completeness_result.completeness_pct

        # Penalize if minimum points not met
        if not completeness_result.min_points_met:
            score *= 0.5

        return float(min(100, max(0, score)))

    def _classify_score(self, score: float) -> str:
        """Classify quality score.

        Args:
            score: Overall quality score

        Returns:
            Status: 'excellent', 'good', 'fair', 'poor'
        """
        if score >= self.excellent_threshold:
            return 'excellent'
        elif score >= self.good_threshold:
            return 'good'
        elif score >= self.fair_threshold:
            return 'fair'
        else:
            return 'poor'

    def _identify_issues(
        self,
        outlier_results: list[OutlierResult],
        freshness_result: FreshnessResult,
        completeness_result: CompletenessResult,
    ) -> list[str]:
        """Identify quality issues.

        Args:
            outlier_results: Outlier detection results
            freshness_result: Freshness monitoring result
            completeness_result: Completeness validation result

        Returns:
            List of issue descriptions
        """
        issues = []

        # Outlier issues
        for result in outlier_results:
            if result.severity in ['medium', 'high']:
                issues.append(
                    f"Outliers in {result.field}: {result.outlier_count} "
                    f"({result.severity} severity)"
                )

        # Freshness issues
        if freshness_result.status != 'fresh':
            issues.append(
                f"Stale data: {freshness_result.age_minutes:.1f} minutes old "
                f"({freshness_result.status})"
            )

        if freshness_result.gaps_detected > 0:
            issues.append(
                f"Time-series gaps: {freshness_result.gaps_detected} detected"
            )

        # Completeness issues
        if completeness_result.completeness_pct < 95.0:
            issues.append(
                f"Incomplete data: {completeness_result.completeness_pct:.1f}% complete "
                f"({completeness_result.status})"
            )

        if completeness_result.missing_fields:
            for field, count in completeness_result.missing_fields.items():
                if count > 0:
                    issues.append(f"Missing {field}: {count} values")

        if not completeness_result.min_points_met:
            issues.append(
                f"Insufficient data: {completeness_result.total_rows} rows "
                f"(minimum required varies by feature)"
            )

        return issues

    def _generate_recommendations(
        self,
        issues: list[str],
        outlier_score: float,
        freshness_score: float,
        completeness_score: float,
    ) -> list[str]:
        """Generate actionable recommendations.

        Args:
            issues: List of identified issues
            outlier_score: Outlier component score
            freshness_score: Freshness component score
            completeness_score: Completeness component score

        Returns:
            List of recommendations
        """
        recommendations = []

        # No issues
        if not issues:
            recommendations.append("Data quality is excellent - no action needed")
            return recommendations

        # Outlier recommendations
        if outlier_score < 70:
            recommendations.append(
                "Clean outliers using interpolation or winsorization"
            )
            recommendations.append(
                "Verify data source for potential anomalies"
            )

        # Freshness recommendations
        if freshness_score < 70:
            recommendations.append(
                "Refresh data from source immediately"
            )
            recommendations.append(
                "Increase data collection frequency"
            )
            recommendations.append(
                "Set up alerts for stale data detection"
            )

        # Completeness recommendations
        if completeness_score < 70:
            recommendations.append(
                "Fill missing values using forward fill or interpolation"
            )
            recommendations.append(
                "Extend data collection period to meet minimum points"
            )
            recommendations.append(
                "Validate data pipeline for gaps"
            )

        return recommendations

    def _create_poor_score(self, symbol: str, reason: str) -> QualityScore:
        """Create poor quality score.

        Args:
            symbol: Trading symbol
            reason: Reason for poor score

        Returns:
            QualityScore with poor status
        """
        return QualityScore(
            symbol=symbol,
            overall_score=0.0,
            component_scores={
                'outlier': 0.0,
                'freshness': 0.0,
                'completeness': 0.0,
            },
            status='poor',
            issues=[reason],
            recommendations=["Collect data before proceeding"],
            timestamp=datetime.now(),
        )

    def get_quality_summary(
        self,
        scores: dict[str, QualityScore],
    ) -> dict[str, Any]:
        """Generate summary statistics for quality scores.

        Args:
            scores: Dictionary of QualityScore objects

        Returns:
            Summary dictionary
        """
        if not scores:
            return {
                'total_symbols': 0,
                'excellent_count': 0,
                'good_count': 0,
                'fair_count': 0,
                'poor_count': 0,
            }

        excellent = [s for s in scores.values() if s.status == 'excellent']
        good = [s for s in scores.values() if s.status == 'good']
        fair = [s for s in scores.values() if s.status == 'fair']
        poor = [s for s in scores.values() if s.status == 'poor']

        avg_score = np.mean([s.overall_score for s in scores.values()])
        min_score = min([s.overall_score for s in scores.values()])

        total_issues = sum(len(s.issues) for s in scores.values())

        return {
            'total_symbols': len(scores),
            'excellent_count': len(excellent),
            'good_count': len(good),
            'fair_count': len(fair),
            'poor_count': len(poor),
            'avg_quality_score': float(avg_score),
            'min_quality_score': float(min_score),
            'total_issues_detected': total_issues,
            'poor_symbols': [s.symbol for s in poor],
            'fair_symbols': [s.symbol for s in fair],
        }
