"""
Quality Collector - Wrapper for QualityScorer with Prometheus Metrics

This module provides a collector that wraps the QualityScorer and automatically
updates Prometheus metrics after each quality check. It includes:
- Timer decorator for measuring quality check duration
- Batch collection for multiple symbols
- Automatic metric updates for all validator results
- Integration with TimescaleDB for historical analysis

Usage:
    from metrics.quality_collector import QualityCollector

    collector = QualityCollector()
    result = await collector.check_quality('BTCUSDT', market_data)
    # Metrics are automatically updated

    # Batch collection
    results = await collector.check_quality_batch(['BTCUSDT', 'ETHUSDT'], data_dict)
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from metrics.quality_metrics import (
    record_quality_check_duration,
    update_metrics_from_completeness_result,
    update_metrics_from_freshness_result,
    update_metrics_from_outlier_results,  # Takes List[OutlierResult]
    update_metrics_from_quality_score,
    update_outlier_metrics,  # For single outlier updates
)
from validators.completeness_validator import CompletenessResult, CompletenessValidator
from validators.freshness_monitor import FreshnessMonitor, FreshnessResult
from validators.outlier_detector import OutlierDetector, OutlierResult
from validators.quality_scorer import QualityScore, QualityScorer

logger = logging.getLogger(__name__)


class QualityCollector:
    """
    Collector that wraps QualityScorer and automatically updates Prometheus metrics.

    This class:
    - Wraps the QualityScorer from Phase 5.5
    - Automatically updates Prometheus metrics after each quality check
    - Records duration metrics for performance monitoring
    - Supports batch collection for multiple symbols
    - Provides integration points for TimescaleDB storage

    Attributes:
        quality_scorer (QualityScorer): The wrapped quality scorer
        outlier_detector (OutlierDetector): Outlier detection validator
        freshness_monitor (FreshnessMonitor): Freshness monitoring validator
        completeness_validator (CompletenessValidator): Completeness validation validator
        enable_metrics (bool): Whether to update Prometheus metrics
        enable_storage (bool): Whether to store results in TimescaleDB
    """

    def __init__(
        self,
        outlier_threshold: float = 3.0,
        freshness_threshold: timedelta = timedelta(minutes=15),
        completeness_threshold: float = 0.9,
        enable_metrics: bool = True,
        enable_storage: bool = False
    ):
        """
        Initialize the QualityCollector.

        Args:
            outlier_threshold: Z-score threshold for outlier detection
            freshness_threshold: Maximum data age before considered stale
            completeness_threshold: Minimum completeness percentage
            enable_metrics: Whether to update Prometheus metrics
            enable_storage: Whether to store results in TimescaleDB
        """
        # Initialize validators (for individual use)
        self.outlier_detector = OutlierDetector(threshold=outlier_threshold)
        # Convert timedelta to minutes for freshness monitor
        freshness_minutes = freshness_threshold.total_seconds() / 60
        self.freshness_monitor = FreshnessMonitor(
            warning_threshold=freshness_minutes * 0.5,
            critical_threshold=freshness_minutes
        )
        # Default required fields for OHLCV data
        required_fields = ['open', 'high', 'low', 'close', 'volume']
        self.completeness_validator = CompletenessValidator(
            required_fields=required_fields,
            excellent_threshold=completeness_threshold * 100  # Convert 0.9 to 90%
        )

        # Quality scorer creates its own validators internally
        self.quality_scorer = QualityScorer()

        self.enable_metrics = enable_metrics
        self.enable_storage = enable_storage

        logger.info(
            "QualityCollector initialized: outlier_threshold=%.2f, "
            "freshness_threshold=%s, completeness_threshold=%.2f, "
            "metrics=%s, storage=%s",
            outlier_threshold,
            freshness_threshold,
            completeness_threshold,
            enable_metrics,
            enable_storage
        )

    def check_quality(
        self,
        symbol: str,
        data: dict[str, Any],
        timestamp: Optional[datetime] = None
    ) -> QualityScore:
        """
        Check data quality for a single symbol and update metrics.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            data: Market data dictionary with OHLCV fields
            timestamp: Data timestamp (defaults to now)

        Returns:
            QualityScore: Comprehensive quality assessment

        Side Effects:
            - Updates Prometheus metrics if enable_metrics=True
            - Stores results in TimescaleDB if enable_storage=True
        """
        start_time = time.time()

        try:
            # Run quality check
            # Perform quality scoring
            result = self.quality_scorer.score(data, symbol=symbol, frequency='1m')

            # Record duration
            duration = time.time() - start_time
            if self.enable_metrics:
                record_quality_check_duration(symbol, duration)

            # Update Prometheus metrics
            if self.enable_metrics:
                self._update_all_metrics(symbol, result)

            # Store in TimescaleDB
            if self.enable_storage:
                self._store_result(symbol, result)

            logger.debug(
                "Quality check completed: symbol=%s, score=%.2f, duration=%.3fs",
                symbol, result.overall_score, duration
            )

            return result

        except Exception as e:
            logger.error("Quality check failed for %s: %s", symbol, e, exc_info=True)
            raise

    async def check_quality_batch(
        self,
        symbols: list[str],
        data_dict: dict[str, dict[str, Any]],
        timestamp: Optional[datetime] = None
    ) -> dict[str, QualityScore]:
        """
        Check data quality for multiple symbols in batch.

        Args:
            symbols: List of trading pair symbols
            data_dict: Dictionary mapping symbol -> market data
            timestamp: Data timestamp (defaults to now)

        Returns:
            Dictionary mapping symbol -> QualityScore

        Side Effects:
            - Updates Prometheus metrics for all symbols if enable_metrics=True
            - Stores all results in TimescaleDB if enable_storage=True
        """
        results = {}

        for symbol in symbols:
            if symbol not in data_dict:
                logger.warning("No data for symbol %s, skipping", symbol)
                continue

            try:
                result = self.check_quality(symbol, data_dict[symbol], timestamp)
                results[symbol] = result
            except Exception as e:
                logger.error("Batch quality check failed for %s: %s", symbol, e)
                # Continue with other symbols
                continue

        logger.info(
            "Batch quality check completed: %d/%d symbols processed",
            len(results), len(symbols)
        )

        return results

    def check_outliers(
        self,
        symbol: str,
        data: dict[str, Any]
    ) -> OutlierResult:
        """
        Check for outliers and update metrics.

        Args:
            symbol: Trading pair symbol
            data: Market data dictionary

        Returns:
            OutlierResult: Outlier detection results
        """
        result = self.outlier_detector.detect_outliers(symbol, data)

        if self.enable_metrics and result.has_outliers:
            # Update metrics for each outlier field
            for field in result.outlier_fields:
                # Handle both enum and string severity
                severity_value = result.severity.value if hasattr(result.severity, 'value') else result.severity
                update_outlier_metrics(
                    symbol=symbol,
                    field=field,
                    severity=severity_value
                )

        return result

    def check_freshness(
        self,
        symbol: str,
        timestamp: datetime,
        current_time: Optional[datetime] = None
    ) -> FreshnessResult:
        """
        Check data freshness and update metrics.

        Args:
            symbol: Trading pair symbol
            timestamp: Data timestamp
            current_time: Current time (defaults to now)

        Returns:
            FreshnessResult: Freshness check results
        """
        result = self.freshness_monitor.check_freshness(symbol, timestamp, current_time)

        if self.enable_metrics:
            update_metrics_from_freshness_result(symbol, result)

        return result

    def check_completeness(
        self,
        symbol: str,
        data: dict[str, Any]
    ) -> CompletenessResult:
        """
        Check data completeness and update metrics.

        Args:
            symbol: Trading pair symbol
            data: Market data dictionary

        Returns:
            CompletenessResult: Completeness validation results
        """
        result = self.completeness_validator.validate_completeness(symbol, data)

        if self.enable_metrics:
            update_metrics_from_completeness_result(symbol, result)

        return result

    def _update_all_metrics(self, symbol: str, result: QualityScore) -> None:
        """
        Update all Prometheus metrics from QualityScore.

        Args:
            symbol: Trading pair symbol
            result: Quality score result
        """
        # Update overall quality score (function expects just the score object)
        update_metrics_from_quality_score(result)

        logger.debug(f"Updated all metrics for {symbol}")

    def _store_result(self, symbol: str, result: QualityScore) -> None:
        """
        Store quality check result in TimescaleDB.

        Args:
            symbol: Trading pair symbol
            result: Quality score to store
        """
        try:
            # Import here to avoid circular dependencies
            from datetime import datetime

            from database.connection import insert_quality_metric

            # Prepare data for insertion using only QualityScore attributes
            data = {
                'time': result.timestamp if hasattr(result, 'timestamp') else datetime.now(),
                'symbol': symbol,
                'overall_score': result.overall_score,
                'status': result.status,
                'outlier_score': result.component_scores.get('outlier', None),
                'freshness_score': result.component_scores.get('freshness', None),
                'completeness_score': result.component_scores.get('completeness', None),
                'outlier_count': 0,  # Not available in QualityScore
                'outlier_severity': None,  # Not available in QualityScore
                'freshness_age_seconds': None,  # Not available in QualityScore
                'completeness_percentage': None,  # Not available in QualityScore
                'issues': result.issues if hasattr(result, 'issues') else [],
                'issue_count': len(result.issues) if hasattr(result, 'issues') else 0,
                'check_duration_ms': None,  # Set externally if needed
                'collector_version': 'v1.0'
            }

            # Insert using database utility
            insert_quality_metric(data)
            logger.debug("Stored quality result for %s in TimescaleDB", symbol)

        except Exception as e:
            logger.error("Failed to store quality result for %s: %s", symbol, e, exc_info=True)
            # Don't raise - storage failure shouldn't break quality checks


def create_quality_collector(
    outlier_threshold: float = 3.0,
    freshness_minutes: int = 15,
    completeness_threshold: float = 0.9,
    enable_metrics: bool = True,
    enable_storage: bool = False
) -> QualityCollector:
    """
    Factory function to create a QualityCollector with standard settings.

    Args:
        outlier_threshold: Z-score threshold for outliers (default: 3.0)
        freshness_minutes: Maximum data age in minutes (default: 15)
        completeness_threshold: Minimum completeness percentage (default: 0.9)
        enable_metrics: Whether to update Prometheus metrics (default: True)
        enable_storage: Whether to store in TimescaleDB (default: False)

    Returns:
        QualityCollector: Configured collector instance

    Example:
        >>> collector = create_quality_collector(freshness_minutes=30)
        >>> result = collector.check_quality('BTCUSDT', market_data)
    """
    return QualityCollector(
        outlier_threshold=outlier_threshold,
        freshness_threshold=timedelta(minutes=freshness_minutes),
        completeness_threshold=completeness_threshold,
        enable_metrics=enable_metrics,
        enable_storage=enable_storage
    )
