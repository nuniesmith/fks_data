"""Prometheus metrics for data quality validation.

Exposes metrics from validators:
- data_quality_score: Overall quality score (0-100)
- data_outlier_count: Number of outliers detected
- data_freshness_seconds: Age of most recent data
- data_completeness_percentage: Completeness percentage
- data_quality_issues: Number of quality issues detected

Metrics use labels: symbol, status, severity

Phase: AI Enhancement Plan Phase 5.6 - Metrics Integration
"""

import logging
from typing import Dict, List, Optional

from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)


# ============================================================================
# Gauge Metrics (Current State)
# ============================================================================

quality_score = Gauge(
    'data_quality_score',
    'Overall data quality score (0-100)',
    ['symbol', 'status'],
)

outlier_score = Gauge(
    'data_outlier_score',
    'Outlier component score (0-100)',
    ['symbol'],
)

freshness_score = Gauge(
    'data_freshness_score',
    'Freshness component score (0-100)',
    ['symbol'],
)

completeness_score = Gauge(
    'data_completeness_score',
    'Completeness component score (0-100)',
    ['symbol'],
)

freshness_age_seconds = Gauge(
    'data_freshness_age_seconds',
    'Age of most recent data in seconds',
    ['symbol', 'status'],
)

completeness_percentage = Gauge(
    'data_completeness_percentage',
    'Percentage of complete rows (0-100)',
    ['symbol', 'status'],
)


# ============================================================================
# Counter Metrics (Cumulative)
# ============================================================================

outlier_count = Counter(
    'data_outlier_count_total',
    'Total number of outliers detected',
    ['symbol', 'field', 'severity'],
)

stale_data_detected = Counter(
    'data_stale_data_detected_total',
    'Total number of times stale data detected',
    ['symbol', 'severity'],
)

quality_issues_detected = Counter(
    'data_quality_issues_total',
    'Total number of quality issues detected',
    ['symbol', 'issue_type'],
)

quality_checks_performed = Counter(
    'data_quality_checks_total',
    'Total number of quality checks performed',
    ['symbol'],
)


# ============================================================================
# Histogram Metrics (Distributions)
# ============================================================================

quality_check_duration = Histogram(
    'data_quality_check_duration_seconds',
    'Time spent performing quality checks',
    ['symbol'],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


# ============================================================================
# Metric Update Functions
# ============================================================================

def update_quality_metrics(
    symbol: str,
    quality_score_value: float,
    status: str,
    component_scores: dict[str, float],
    issues: list[str],
):
    """Update quality metrics from QualityScore result.

    Args:
        symbol: Trading symbol
        quality_score_value: Overall quality score (0-100)
        status: Quality status (excellent, good, fair, poor)
        component_scores: Dictionary of component scores
        issues: List of quality issues detected
    """
    # Update gauge metrics
    quality_score.labels(symbol=symbol, status=status).set(quality_score_value)

    if 'outlier' in component_scores:
        outlier_score.labels(symbol=symbol).set(component_scores['outlier'])

    if 'freshness' in component_scores:
        freshness_score.labels(symbol=symbol).set(component_scores['freshness'])

    if 'completeness' in component_scores:
        completeness_score.labels(symbol=symbol).set(component_scores['completeness'])

    # Update issue counter
    for issue in issues:
        # Classify issue type
        if 'outlier' in issue.lower():
            issue_type = 'outlier'
        elif 'stale' in issue.lower() or 'fresh' in issue.lower():
            issue_type = 'freshness'
        elif 'incomplete' in issue.lower() or 'missing' in issue.lower():
            issue_type = 'completeness'
        else:
            issue_type = 'other'

        quality_issues_detected.labels(symbol=symbol, issue_type=issue_type).inc()

    # Increment check counter
    quality_checks_performed.labels(symbol=symbol).inc()

    logger.debug(
        f"Updated quality metrics for {symbol}: "
        f"score={quality_score_value:.1f}, status={status}, issues={len(issues)}"
    )


def update_outlier_metrics(
    symbol: str,
    field: str,
    outlier_count_value: int,
    severity: str,
):
    """Update outlier detection metrics.

    Args:
        symbol: Trading symbol
        field: Field name (close, volume, etc.)
        outlier_count_value: Number of outliers detected
        severity: Severity level (low, medium, high)
    """
    if outlier_count_value > 0:
        outlier_count.labels(
            symbol=symbol,
            field=field,
            severity=severity,
        ).inc(outlier_count_value)

        logger.debug(
            f"Updated outlier metrics for {symbol}.{field}: "
            f"count={outlier_count_value}, severity={severity}"
        )


def update_freshness_metrics(
    symbol: str,
    age_seconds: float,
    status: str,
):
    """Update freshness monitoring metrics.

    Args:
        symbol: Trading symbol
        age_seconds: Age of most recent data in seconds
        status: Freshness status (fresh, warning, critical)
    """
    freshness_age_seconds.labels(symbol=symbol, status=status).set(age_seconds)

    # Increment stale data counter if not fresh
    if status in ['warning', 'critical']:
        severity = 'warning' if status == 'warning' else 'critical'
        stale_data_detected.labels(symbol=symbol, severity=severity).inc()

    logger.debug(
        f"Updated freshness metrics for {symbol}: "
        f"age={age_seconds:.1f}s, status={status}"
    )


def update_completeness_metrics(
    symbol: str,
    completeness_pct: float,
    status: str,
):
    """Update completeness validation metrics.

    Args:
        symbol: Trading symbol
        completeness_pct: Completeness percentage (0-100)
        status: Completeness status (excellent, good, fair, poor)
    """
    completeness_percentage.labels(symbol=symbol, status=status).set(completeness_pct)

    logger.debug(
        f"Updated completeness metrics for {symbol}: "
        f"completeness={completeness_pct:.1f}%, status={status}"
    )


def record_quality_check_duration(symbol: str, duration_seconds: float):
    """Record time spent performing quality check.

    Args:
        symbol: Trading symbol
        duration_seconds: Duration in seconds
    """
    quality_check_duration.labels(symbol=symbol).observe(duration_seconds)


# ============================================================================
# Batch Update Functions
# ============================================================================

def update_metrics_from_quality_score(quality_score_obj):
    """Update all metrics from QualityScore object.

    Args:
        quality_score_obj: QualityScore object from quality_scorer
    """
    update_quality_metrics(
        symbol=quality_score_obj.symbol,
        quality_score_value=quality_score_obj.overall_score,
        status=quality_score_obj.status,
        component_scores=quality_score_obj.component_scores,
        issues=quality_score_obj.issues,
    )


def update_metrics_from_outlier_results(symbol: str, outlier_results: list):
    """Update metrics from OutlierResult list.

    Args:
        symbol: Trading symbol
        outlier_results: List of OutlierResult objects
    """
    for result in outlier_results:
        update_outlier_metrics(
            symbol=symbol,
            field=result.field,
            outlier_count_value=result.outlier_count,
            severity=result.severity,
        )


def update_metrics_from_freshness_result(freshness_result):
    """Update metrics from FreshnessResult object.

    Args:
        freshness_result: FreshnessResult object
    """
    update_freshness_metrics(
        symbol=freshness_result.symbol,
        age_seconds=freshness_result.age_seconds,
        status=freshness_result.status,
    )


def update_metrics_from_completeness_result(completeness_result):
    """Update metrics from CompletenessResult object.

    Args:
        completeness_result: CompletenessResult object
    """
    update_completeness_metrics(
        symbol=completeness_result.symbol,
        completeness_pct=completeness_result.completeness_pct,
        status=completeness_result.status,
    )


# ============================================================================
# Reset Functions (for testing)
# ============================================================================

def reset_all_quality_metrics():
    """Reset all quality metrics to zero.

    WARNING: Only use in testing. Prometheus metrics are cumulative.
    """
    logger.warning("Resetting all quality metrics - use only in testing!")

    # Note: Prometheus client doesn't support resetting metrics directly
    # This is a placeholder for testing documentation
    pass
