"""Outlier detection for market data quality validation.

This module provides statistical outlier detection for:
- Price anomalies (sudden spikes/drops)
- Volume irregularities
- Spread/slippage outliers

Methods:
- Z-score: Detects values >N standard deviations from mean
- IQR (Interquartile Range): Robust to extreme values
- MAD (Median Absolute Deviation): Robust alternative to std dev

Phase: AI Enhancement Plan Phase 5.5 - Data Quality Validation
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class OutlierResult:
    """Result of outlier detection.

    Attributes:
        field: Field name (e.g., 'close', 'volume')
        outlier_indices: List of indices where outliers detected
        outlier_count: Number of outliers found
        method: Detection method used
        threshold: Threshold value used
        severity: 'low', 'medium', 'high' based on outlier count
    """
    field: str
    outlier_indices: list[int]
    outlier_count: int
    method: str
    threshold: float
    severity: str

    @property
    def outlier_percentage(self) -> float:
        """Calculate percentage of outliers."""
        if not hasattr(self, '_total_points'):
            return 0.0
        return (self.outlier_count / self._total_points) * 100 if self._total_points > 0 else 0.0


class OutlierDetector:
    """Statistical outlier detection for market data.

    Supports multiple detection methods:
    - Z-score: Standard deviation based
    - IQR: Interquartile range based (robust)
    - MAD: Median absolute deviation based (most robust)

    Example:
        >>> detector = OutlierDetector(method='zscore', threshold=3.0)
        >>> results = detector.detect(df, fields=['close', 'volume'])
        >>> for result in results:
        ...     if result.severity == 'high':
        ...         print(f"High severity outliers in {result.field}")
    """

    def __init__(
        self,
        method: str = 'zscore',
        threshold: float = 3.0,
        min_periods: int = 20,
        window_size: Optional[int] = None,
    ):
        """Initialize outlier detector.

        Args:
            method: Detection method ('zscore', 'iqr', 'mad')
            threshold: Threshold for outlier detection
                - zscore: number of standard deviations (default 3.0)
                - iqr: IQR multiplier (default 1.5)
                - mad: MAD multiplier (default 3.0)
            min_periods: Minimum data points required for detection
            window_size: Rolling window size (None = use all data)
        """
        self.method = method.lower()
        self.threshold = threshold
        self.min_periods = min_periods
        self.window_size = window_size

        if self.method not in ['zscore', 'iqr', 'mad']:
            raise ValueError(f"Unknown method: {method}. Use 'zscore', 'iqr', or 'mad'")

        logger.info(f"OutlierDetector initialized (method={method}, threshold={threshold})")

    def detect(
        self,
        data: pd.DataFrame,
        fields: Optional[list[str]] = None,
    ) -> list[OutlierResult]:
        """Detect outliers in specified fields.

        Args:
            data: DataFrame with OHLCV data
            fields: Fields to check (default: ['close', 'volume'])

        Returns:
            List of OutlierResult objects
        """
        if len(data) < self.min_periods:
            logger.warning(f"Insufficient data points: {len(data)} < {self.min_periods}")
            return []

        if fields is None:
            fields = ['close', 'volume']

        results = []

        for field in fields:
            if field not in data.columns:
                logger.warning(f"Field '{field}' not found in data")
                continue

            outlier_indices = self._detect_field(data[field])
            severity = self._classify_severity(len(outlier_indices), len(data))

            result = OutlierResult(
                field=field,
                outlier_indices=outlier_indices,
                outlier_count=len(outlier_indices),
                method=self.method,
                threshold=self.threshold,
                severity=severity,
            )
            result._total_points = len(data)

            results.append(result)

            if result.outlier_count > 0:
                logger.info(
                    f"Detected {result.outlier_count} outliers in '{field}' "
                    f"({result.outlier_percentage:.1f}%, severity={severity})"
                )

        return results

    def _detect_field(self, series: pd.Series) -> list[int]:
        """Detect outliers in a single field.

        Args:
            series: Pandas Series to analyze

        Returns:
            List of indices where outliers detected
        """
        if self.method == 'zscore':
            return self._zscore_method(series)
        elif self.method == 'iqr':
            return self._iqr_method(series)
        elif self.method == 'mad':
            return self._mad_method(series)
        else:
            return []

    def _zscore_method(self, series: pd.Series) -> list[int]:
        """Z-score outlier detection.

        Flags values >threshold standard deviations from mean.

        Args:
            series: Data series

        Returns:
            Outlier indices
        """
        if self.window_size:
            # Rolling window z-score
            mean = series.rolling(window=self.window_size, min_periods=self.min_periods).mean()
            std = series.rolling(window=self.window_size, min_periods=self.min_periods).std()
        else:
            # Global z-score
            mean = series.mean()
            std = series.std()

        # Avoid division by zero
        if isinstance(std, pd.Series):
            std = std.replace(0, np.nan)
        elif std == 0:
            return []

        z_scores = np.abs((series - mean) / std)
        outliers = z_scores > self.threshold

        return outliers[outliers].index.tolist()

    def _iqr_method(self, series: pd.Series) -> list[int]:
        """IQR (Interquartile Range) outlier detection.

        Flags values outside [Q1 - threshold*IQR, Q3 + threshold*IQR].
        More robust to extreme values than z-score.

        Args:
            series: Data series

        Returns:
            Outlier indices
        """
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - (self.threshold * IQR)
        upper_bound = Q3 + (self.threshold * IQR)

        outliers = (series < lower_bound) | (series > upper_bound)

        return outliers[outliers].index.tolist()

    def _mad_method(self, series: pd.Series) -> list[int]:
        """MAD (Median Absolute Deviation) outlier detection.

        Most robust method, uses median instead of mean.
        Flags values >threshold MADs from median.

        Args:
            series: Data series

        Returns:
            Outlier indices
        """
        median = series.median()
        mad = np.median(np.abs(series - median))

        # Avoid division by zero
        if mad == 0:
            return []

        # Modified z-score using MAD
        # Factor 1.4826 makes MAD comparable to standard deviation for normal distribution
        modified_z_score = 0.6745 * (series - median) / mad
        outliers = np.abs(modified_z_score) > self.threshold

        return outliers[outliers].index.tolist()

    def _classify_severity(self, outlier_count: int, total_points: int) -> str:
        """Classify outlier severity based on percentage.

        Args:
            outlier_count: Number of outliers
            total_points: Total data points

        Returns:
            Severity level ('low', 'medium', 'high')
        """
        percentage = (outlier_count / total_points) * 100 if total_points > 0 else 0

        if percentage > 10:
            return 'high'
        elif percentage > 5:
            return 'medium'
        else:
            return 'low'

    def get_outlier_summary(self, results: list[OutlierResult]) -> dict[str, Any]:
        """Generate summary statistics for outlier detection results.

        Args:
            results: List of OutlierResult objects

        Returns:
            Summary dictionary
        """
        total_outliers = sum(r.outlier_count for r in results)
        high_severity = [r for r in results if r.severity == 'high']
        medium_severity = [r for r in results if r.severity == 'medium']

        return {
            'total_outliers': total_outliers,
            'fields_checked': len(results),
            'high_severity_count': len(high_severity),
            'medium_severity_count': len(medium_severity),
            'high_severity_fields': [r.field for r in high_severity],
            'method': self.method,
            'threshold': self.threshold,
        }

    def clean_outliers(
        self,
        data: pd.DataFrame,
        results: list[OutlierResult],
        method: str = 'remove',
    ) -> pd.DataFrame:
        """Remove or replace outliers in data.

        Args:
            data: Original DataFrame
            results: Outlier detection results
            method: Cleaning method
                - 'remove': Drop outlier rows
                - 'interpolate': Linear interpolation
                - 'winsorize': Cap at threshold values

        Returns:
            Cleaned DataFrame
        """
        cleaned = data.copy()

        if method == 'remove':
            # Collect all outlier indices
            all_outlier_indices = set()
            for result in results:
                all_outlier_indices.update(result.outlier_indices)

            # Drop outlier rows
            cleaned = cleaned.drop(index=list(all_outlier_indices))
            logger.info(f"Removed {len(all_outlier_indices)} outlier rows")

        elif method == 'interpolate':
            for result in results:
                if result.field in cleaned.columns:
                    # Mark outliers as NaN
                    cleaned.loc[result.outlier_indices, result.field] = np.nan
                    # Interpolate
                    cleaned[result.field] = cleaned[result.field].interpolate(method='linear')

            logger.info(f"Interpolated outliers in {len(results)} fields")

        elif method == 'winsorize':
            for result in results:
                if result.field in cleaned.columns:
                    # Calculate bounds
                    Q1 = data[result.field].quantile(0.25)
                    Q3 = data[result.field].quantile(0.75)
                    IQR = Q3 - Q1
                    lower = Q1 - (1.5 * IQR)
                    upper = Q3 + (1.5 * IQR)

                    # Cap values
                    cleaned[result.field] = cleaned[result.field].clip(lower=lower, upper=upper)

            logger.info(f"Winsorized outliers in {len(results)} fields")

        return cleaned
