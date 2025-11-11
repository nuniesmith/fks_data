"""Metrics module for data quality monitoring.

Phase: AI Enhancement Plan Phase 5.6 - Metrics Integration

This module provides:
- QualityCollector: Main collector class with automatic metrics updates
- Prometheus metrics for quality scores, outliers, freshness, completeness
- Integration functions for batch updates from validator results

Usage:
    from metrics import QualityCollector

    collector = QualityCollector()
    result = collector.check_quality('BTCUSDT', market_data)
"""

from .quality_collector import QualityCollector, create_quality_collector
from .quality_metrics import (
    completeness_percentage,
    completeness_score,
    freshness_age_seconds,
    freshness_score,
    # Counter metrics
    outlier_count,
    outlier_score,
    # Histogram metrics
    quality_check_duration,
    quality_checks_performed,
    quality_issues_detected,
    # Gauge metrics
    quality_score,
    record_quality_check_duration,
    stale_data_detected,
    update_completeness_metrics,
    update_freshness_metrics,
    update_metrics_from_completeness_result,
    update_metrics_from_freshness_result,
    update_metrics_from_outlier_results,  # Note: takes List[OutlierResult]
    # Batch update functions
    update_metrics_from_quality_score,
    update_outlier_metrics,
    # Update functions
    update_quality_metrics,
)

__all__ = [
    # Collector
    'QualityCollector',
    'create_quality_collector',

    # Gauge metrics
    'quality_score',
    'outlier_score',
    'freshness_score',
    'completeness_score',
    'freshness_age_seconds',
    'completeness_percentage',

    # Counter metrics
    'outlier_count',
    'stale_data_detected',
    'quality_issues_detected',
    'quality_checks_performed',

    # Histogram metrics
    'quality_check_duration',

    # Update functions
    'update_quality_metrics',
    'update_outlier_metrics',
    'update_freshness_metrics',
    'update_completeness_metrics',
    'record_quality_check_duration',

    # Batch update functions
    'update_metrics_from_quality_score',
    'update_metrics_from_outlier_results',  # Takes List[OutlierResult]
    'update_metrics_from_freshness_result',
    'update_metrics_from_completeness_result',
]
