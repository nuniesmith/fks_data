"""Data completeness validation for OHLCV time-series.

This module validates:
- Required fields present (OHLCV)
- Null/missing value detection
- Time-series gaps
- Minimum data points for feature computation

Completeness Levels:
- Excellent: >99% complete
- Good: 95-99% complete
- Fair: 90-95% complete
- Poor: <90% complete

Phase: AI Enhancement Plan Phase 5.5 - Data Quality Validation
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CompletenessResult:
    """Result of completeness validation.

    Attributes:
        symbol: Trading symbol
        total_rows: Total number of rows
        complete_rows: Rows with all required fields
        completeness_pct: Percentage of complete rows
        missing_fields: Dictionary of field -> missing count
        gaps_detected: Number of time-series gaps
        min_points_met: Whether minimum data points requirement is met
        status: 'excellent', 'good', 'fair', 'poor'
    """
    symbol: str
    total_rows: int
    complete_rows: int
    completeness_pct: float
    missing_fields: dict[str, int]
    gaps_detected: int
    min_points_met: bool
    status: str

    @property
    def is_complete(self) -> bool:
        """Check if data meets completeness threshold (>95%)."""
        return self.completeness_pct >= 95.0

    @property
    def has_critical_gaps(self) -> bool:
        """Check if data has critical gaps (>5% missing)."""
        return self.completeness_pct < 95.0


class CompletenessValidator:
    """Validate data completeness for OHLCV time-series.

    Checks for:
    - Required fields present
    - Missing/null values
    - Time-series gaps
    - Minimum data points

    Example:
        >>> validator = CompletenessValidator(required_fields=['open', 'high', 'low', 'close', 'volume'])
        >>> result = validator.validate(df, symbol='BTCUSDT', min_points=100)
        >>> if not result.is_complete:
        ...     print(f"WARNING: {result.symbol} only {result.completeness_pct:.1f}% complete")
    """

    def __init__(
        self,
        required_fields: Optional[list[str]] = None,
        excellent_threshold: float = 99.0,
        good_threshold: float = 95.0,
        fair_threshold: float = 90.0,
    ):
        """Initialize completeness validator.

        Args:
            required_fields: List of required column names (default: OHLCV)
            excellent_threshold: Threshold for 'excellent' status (default 99%)
            good_threshold: Threshold for 'good' status (default 95%)
            fair_threshold: Threshold for 'fair' status (default 90%)
        """
        self.required_fields = required_fields or [
            'open', 'high', 'low', 'close', 'volume'
        ]
        self.excellent_threshold = excellent_threshold
        self.good_threshold = good_threshold
        self.fair_threshold = fair_threshold

        logger.info(
            f"CompletenessValidator initialized with fields: {self.required_fields}"
        )

    def validate(
        self,
        data: pd.DataFrame,
        symbol: str = "",
        min_points: int = 50,
        timestamp_col: Optional[str] = 'timestamp',
        expected_frequency: Optional[str] = None,
    ) -> CompletenessResult:
        """Validate data completeness.

        Args:
            data: DataFrame to validate
            symbol: Trading symbol
            min_points: Minimum required data points (default 50)
            timestamp_col: Timestamp column for gap detection
            expected_frequency: Expected data frequency for gap detection

        Returns:
            CompletenessResult object
        """
        if data.empty:
            logger.warning(f"Empty data for symbol: {symbol}")
            return self._create_empty_result(symbol)

        # Check required fields
        missing_cols = self._check_required_fields(data)
        if missing_cols:
            logger.error(f"Missing required columns for {symbol}: {missing_cols}")
            return self._create_incomplete_result(symbol, data, missing_cols)

        # Count missing values per field
        missing_fields = self._count_missing_values(data)

        # Calculate completeness
        total_rows = len(data)
        complete_rows = self._count_complete_rows(data)
        completeness_pct = (complete_rows / total_rows) * 100 if total_rows > 0 else 0.0

        # Check minimum points
        min_points_met = total_rows >= min_points

        # Detect gaps
        gaps = 0
        if timestamp_col and timestamp_col in data.columns and expected_frequency:
            gaps = self._detect_gaps(data[timestamp_col], expected_frequency)

        # Determine status
        status = self._classify_completeness(completeness_pct)

        result = CompletenessResult(
            symbol=symbol,
            total_rows=total_rows,
            complete_rows=complete_rows,
            completeness_pct=completeness_pct,
            missing_fields=missing_fields,
            gaps_detected=gaps,
            min_points_met=min_points_met,
            status=status,
        )

        if status in ['poor', 'fair']:
            logger.warning(
                f"Low completeness for {symbol}: {completeness_pct:.1f}% "
                f"(status={status})"
            )

        if not min_points_met:
            logger.warning(
                f"Insufficient data for {symbol}: {total_rows} < {min_points} points"
            )

        return result

    def validate_multiple(
        self,
        data_dict: dict[str, pd.DataFrame],
        min_points: int = 50,
        timestamp_col: Optional[str] = 'timestamp',
        expected_frequency: Optional[str] = None,
    ) -> dict[str, CompletenessResult]:
        """Validate completeness for multiple symbols.

        Args:
            data_dict: Dictionary mapping symbols to DataFrames
            min_points: Minimum required data points
            timestamp_col: Timestamp column name
            expected_frequency: Expected data frequency

        Returns:
            Dictionary mapping symbols to CompletenessResult objects
        """
        results = {}

        for symbol, data in data_dict.items():
            results[symbol] = self.validate(
                data,
                symbol=symbol,
                min_points=min_points,
                timestamp_col=timestamp_col,
                expected_frequency=expected_frequency,
            )

        return results

    def _check_required_fields(self, data: pd.DataFrame) -> set[str]:
        """Check if all required fields are present.

        Args:
            data: DataFrame to check

        Returns:
            Set of missing field names
        """
        present_fields = set(data.columns)
        required_set = set(self.required_fields)
        missing = required_set - present_fields
        return missing

    def _count_missing_values(self, data: pd.DataFrame) -> dict[str, int]:
        """Count missing values per field.

        Args:
            data: DataFrame to analyze

        Returns:
            Dictionary mapping field -> missing count
        """
        missing = {}

        for field in self.required_fields:
            if field in data.columns:
                missing[field] = int(data[field].isna().sum())

        return missing

    def _count_complete_rows(self, data: pd.DataFrame) -> int:
        """Count rows with all required fields non-null.

        Args:
            data: DataFrame to analyze

        Returns:
            Number of complete rows
        """
        # Get subset of required fields
        subset = [f for f in self.required_fields if f in data.columns]

        if not subset:
            return 0

        # Count rows with no nulls in required fields
        complete = data[subset].notna().all(axis=1).sum()
        return int(complete)

    def _detect_gaps(self, timestamps: pd.Series, frequency: str) -> int:
        """Detect gaps in time-series.

        Args:
            timestamps: Series of timestamps
            frequency: Expected frequency

        Returns:
            Number of gaps detected
        """
        timestamps = pd.to_datetime(timestamps).sort_values()

        if len(timestamps) < 2:
            return 0

        # Create expected range
        expected_range = pd.date_range(
            start=timestamps.min(),
            end=timestamps.max(),
            freq=frequency,
        )

        # Count missing timestamps
        gaps = len(expected_range) - len(timestamps)
        return max(0, gaps)

    def _classify_completeness(self, completeness_pct: float) -> str:
        """Classify completeness percentage.

        Args:
            completeness_pct: Completeness percentage

        Returns:
            Status: 'excellent', 'good', 'fair', 'poor'
        """
        if completeness_pct >= self.excellent_threshold:
            return 'excellent'
        elif completeness_pct >= self.good_threshold:
            return 'good'
        elif completeness_pct >= self.fair_threshold:
            return 'fair'
        else:
            return 'poor'

    def _create_empty_result(self, symbol: str) -> CompletenessResult:
        """Create result for empty data.

        Args:
            symbol: Trading symbol

        Returns:
            CompletenessResult with poor status
        """
        return CompletenessResult(
            symbol=symbol,
            total_rows=0,
            complete_rows=0,
            completeness_pct=0.0,
            missing_fields=dict.fromkeys(self.required_fields, 0),
            gaps_detected=0,
            min_points_met=False,
            status='poor',
        )

    def _create_incomplete_result(
        self,
        symbol: str,
        data: pd.DataFrame,
        missing_cols: set[str],
    ) -> CompletenessResult:
        """Create result for data with missing required columns.

        Args:
            symbol: Trading symbol
            data: DataFrame
            missing_cols: Set of missing column names

        Returns:
            CompletenessResult with poor status
        """
        missing_fields = {col: len(data) for col in missing_cols}

        return CompletenessResult(
            symbol=symbol,
            total_rows=len(data),
            complete_rows=0,
            completeness_pct=0.0,
            missing_fields=missing_fields,
            gaps_detected=0,
            min_points_met=False,
            status='poor',
        )

    def get_completeness_summary(
        self,
        results: dict[str, CompletenessResult],
    ) -> dict[str, Any]:
        """Generate summary statistics for completeness validation.

        Args:
            results: Dictionary of CompletenessResult objects

        Returns:
            Summary dictionary
        """
        if not results:
            return {
                'total_symbols': 0,
                'excellent_count': 0,
                'good_count': 0,
                'fair_count': 0,
                'poor_count': 0,
            }

        excellent = [r for r in results.values() if r.status == 'excellent']
        good = [r for r in results.values() if r.status == 'good']
        fair = [r for r in results.values() if r.status == 'fair']
        poor = [r for r in results.values() if r.status == 'poor']

        avg_completeness = np.mean([r.completeness_pct for r in results.values()])
        total_missing = sum(
            sum(r.missing_fields.values()) for r in results.values()
        )

        return {
            'total_symbols': len(results),
            'excellent_count': len(excellent),
            'good_count': len(good),
            'fair_count': len(fair),
            'poor_count': len(poor),
            'avg_completeness_pct': float(avg_completeness),
            'total_missing_values': total_missing,
            'poor_symbols': [r.symbol for r in poor],
            'fair_symbols': [r.symbol for r in fair],
        }

    def get_incomplete_symbols(
        self,
        results: dict[str, CompletenessResult],
        min_completeness: float = 95.0,
    ) -> list[str]:
        """Get list of incomplete symbols.

        Args:
            results: Completeness results
            min_completeness: Minimum completeness threshold (default 95%)

        Returns:
            List of incomplete symbols
        """
        incomplete = []

        for symbol, result in results.items():
            if result.completeness_pct < min_completeness:
                incomplete.append(symbol)

        return incomplete
