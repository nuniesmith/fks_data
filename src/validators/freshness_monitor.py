"""Data freshness monitoring for detecting stale market data.

This module monitors:
- Time since last update
- Data gaps in time-series
- Symbol-specific staleness
- Alerting for stale data

Thresholds:
- Critical: >15 minutes stale
- Warning: >5 minutes stale
- Info: >1 minute stale

Phase: AI Enhancement Plan Phase 5.5 - Data Quality Validation
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class FreshnessResult:
    """Result of freshness monitoring.

    Attributes:
        symbol: Trading symbol
        last_timestamp: Most recent data timestamp
        age_seconds: Age of most recent data in seconds
        age_minutes: Age in minutes
        status: 'fresh', 'warning', 'critical'
        gaps_detected: Number of gaps in time-series
        expected_frequency: Expected data frequency (e.g., '1m', '1h')
    """
    symbol: str
    last_timestamp: datetime
    age_seconds: float
    age_minutes: float
    status: str
    gaps_detected: int
    expected_frequency: str

    @property
    def is_stale(self) -> bool:
        """Check if data is stale (>15 min old)."""
        return self.age_minutes > 15

    @property
    def needs_refresh(self) -> bool:
        """Check if data needs refresh (>5 min old)."""
        return self.age_minutes > 5


class FreshnessMonitor:
    """Monitor data freshness and detect stale data.

    Monitors time since last update and detects gaps in time-series data.
    Provides alerts based on configurable thresholds.

    Example:
        >>> monitor = FreshnessMonitor(warning_threshold=5, critical_threshold=15)
        >>> result = monitor.check_freshness(df, symbol='BTCUSDT', frequency='1m')
        >>> if result.is_stale:
        ...     print(f"ALERT: {result.symbol} is {result.age_minutes:.1f} minutes old")
    """

    def __init__(
        self,
        warning_threshold: float = 5.0,
        critical_threshold: float = 15.0,
        gap_tolerance: float = 1.5,
    ):
        """Initialize freshness monitor.

        Args:
            warning_threshold: Minutes before warning (default 5)
            critical_threshold: Minutes before critical alert (default 15)
            gap_tolerance: Multiplier for detecting gaps (default 1.5)
                - Gap detected if interval > expected * gap_tolerance
        """
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.gap_tolerance = gap_tolerance

        logger.info(
            f"FreshnessMonitor initialized "
            f"(warning={warning_threshold}m, critical={critical_threshold}m)"
        )

    def check_freshness(
        self,
        data: pd.DataFrame,
        symbol: str = "",
        frequency: str = '1m',
        timestamp_col: str = 'timestamp',
        reference_time: Optional[datetime] = None,
    ) -> FreshnessResult:
        """Check data freshness for a symbol.

        Args:
            data: DataFrame with timestamp column
            symbol: Trading symbol
            frequency: Expected data frequency ('1m', '5m', '1h', etc.)
            timestamp_col: Name of timestamp column
            reference_time: Reference time (default: now)

        Returns:
            FreshnessResult object
        """
        if data.empty:
            logger.warning(f"Empty data for symbol: {symbol}")
            return self._create_empty_result(symbol, frequency)

        if timestamp_col not in data.columns:
            logger.error(f"Timestamp column '{timestamp_col}' not found")
            return self._create_empty_result(symbol, frequency)

        # Get most recent timestamp
        last_timestamp = pd.to_datetime(data[timestamp_col]).max()

        # Calculate age
        if reference_time is None:
            reference_time = datetime.now(last_timestamp.tzinfo) if last_timestamp.tzinfo else datetime.now()

        age = reference_time - last_timestamp
        age_seconds = age.total_seconds()
        age_minutes = age_seconds / 60

        # Determine status
        if age_minutes > self.critical_threshold:
            status = 'critical'
        elif age_minutes > self.warning_threshold:
            status = 'warning'
        else:
            status = 'fresh'

        # Detect gaps
        gaps = self._detect_gaps(data[timestamp_col], frequency)

        result = FreshnessResult(
            symbol=symbol,
            last_timestamp=last_timestamp,
            age_seconds=age_seconds,
            age_minutes=age_minutes,
            status=status,
            gaps_detected=len(gaps),
            expected_frequency=frequency,
        )

        if status != 'fresh':
            logger.warning(
                f"Stale data for {symbol}: {age_minutes:.1f} minutes old "
                f"(status={status})"
            )

        if len(gaps) > 0:
            logger.info(f"Detected {len(gaps)} gaps in {symbol} time-series")

        return result

    def check_multiple(
        self,
        data_dict: dict[str, pd.DataFrame],
        frequency: str = '1m',
        timestamp_col: str = 'timestamp',
    ) -> dict[str, FreshnessResult]:
        """Check freshness for multiple symbols.

        Args:
            data_dict: Dictionary mapping symbols to DataFrames
            frequency: Expected data frequency
            timestamp_col: Timestamp column name

        Returns:
            Dictionary mapping symbols to FreshnessResult objects
        """
        results = {}
        reference_time = datetime.now()

        for symbol, data in data_dict.items():
            results[symbol] = self.check_freshness(
                data,
                symbol=symbol,
                frequency=frequency,
                timestamp_col=timestamp_col,
                reference_time=reference_time,
            )

        return results

    def _detect_gaps(
        self,
        timestamps: pd.Series,
        frequency: str,
    ) -> list[tuple[datetime, datetime]]:
        """Detect gaps in time-series data.

        Args:
            timestamps: Series of timestamps
            frequency: Expected frequency

        Returns:
            List of (gap_start, gap_end) tuples
        """
        timestamps = pd.to_datetime(timestamps).sort_values()

        if len(timestamps) < 2:
            return []

        # Parse frequency
        expected_interval = self._parse_frequency(frequency)
        if expected_interval is None:
            return []

        # Calculate intervals between consecutive timestamps
        intervals = timestamps.diff()[1:]  # Skip first NaT

        # Detect gaps (intervals > expected * tolerance)
        threshold = expected_interval * self.gap_tolerance
        gap_mask = intervals > threshold

        gaps = []
        for idx, is_gap in gap_mask.items():
            if is_gap:
                gap_start = timestamps.iloc[idx - 1]
                gap_end = timestamps.iloc[idx]
                gaps.append((gap_start, gap_end))

        return gaps

    def _parse_frequency(self, frequency: str) -> Optional[timedelta]:
        """Parse frequency string to timedelta.

        Args:
            frequency: Frequency string ('1m', '5m', '1h', '1d')

        Returns:
            Timedelta or None if invalid
        """
        freq_map = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '15m': timedelta(minutes=15),
            '30m': timedelta(minutes=30),
            '1h': timedelta(hours=1),
            '4h': timedelta(hours=4),
            '1d': timedelta(days=1),
        }

        return freq_map.get(frequency.lower())

    def _create_empty_result(self, symbol: str, frequency: str) -> FreshnessResult:
        """Create result for empty/invalid data.

        Args:
            symbol: Trading symbol
            frequency: Expected frequency

        Returns:
            FreshnessResult with critical status
        """
        return FreshnessResult(
            symbol=symbol,
            last_timestamp=datetime.min,
            age_seconds=float('inf'),
            age_minutes=float('inf'),
            status='critical',
            gaps_detected=0,
            expected_frequency=frequency,
        )

    def get_freshness_summary(
        self,
        results: dict[str, FreshnessResult],
    ) -> dict[str, Any]:
        """Generate summary statistics for freshness monitoring.

        Args:
            results: Dictionary of FreshnessResult objects

        Returns:
            Summary dictionary
        """
        if not results:
            return {
                'total_symbols': 0,
                'fresh_count': 0,
                'warning_count': 0,
                'critical_count': 0,
            }

        fresh = [r for r in results.values() if r.status == 'fresh']
        warning = [r for r in results.values() if r.status == 'warning']
        critical = [r for r in results.values() if r.status == 'critical']

        avg_age = np.mean([r.age_minutes for r in results.values() if r.age_minutes != float('inf')])
        max_age = max([r.age_minutes for r in results.values() if r.age_minutes != float('inf')], default=0)

        total_gaps = sum(r.gaps_detected for r in results.values())

        return {
            'total_symbols': len(results),
            'fresh_count': len(fresh),
            'warning_count': len(warning),
            'critical_count': len(critical),
            'avg_age_minutes': float(avg_age) if not np.isnan(avg_age) else 0.0,
            'max_age_minutes': float(max_age),
            'total_gaps_detected': total_gaps,
            'critical_symbols': [r.symbol for r in critical],
            'warning_symbols': [r.symbol for r in warning],
        }

    def get_stale_symbols(
        self,
        results: dict[str, FreshnessResult],
        min_severity: str = 'warning',
    ) -> list[str]:
        """Get list of stale symbols.

        Args:
            results: Freshness results
            min_severity: Minimum severity ('warning' or 'critical')

        Returns:
            List of stale symbols
        """
        stale = []

        for symbol, result in results.items():
            if min_severity == 'critical':
                if result.status == 'critical':
                    stale.append(symbol)
            else:  # warning or higher
                if result.status in ['warning', 'critical']:
                    stale.append(symbol)

        return stale
