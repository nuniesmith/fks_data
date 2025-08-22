"""
Data validation, anomaly detection, cross-validation, and dataset splitting utilities.

Designed to enforce high data quality before training/serving models.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger


REQUIRED_COLS = ["open", "high", "low", "close"]


@dataclass
class QualityReport:
    rows: int
    missing_pct: float
    duplicate_timestamps: int
    outlier_pct: float
    gap_count: int
    timeframe_seconds: Optional[int]
    notes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rows": self.rows,
            "missing_pct": self.missing_pct,
            "duplicate_timestamps": self.duplicate_timestamps,
            "outlier_pct": self.outlier_pct,
            "gap_count": self.gap_count,
            "timeframe_seconds": self.timeframe_seconds,
            "notes": self.notes,
        }


def _detect_timeframe_seconds(df: pd.DataFrame, dt_col: str) -> Optional[int]:
    if df.empty:
        return None
    s = pd.to_datetime(df[dt_col]).sort_values().drop_duplicates()
    if len(s) < 2:
        return None
    diffs = s.diff().dropna().dt.total_seconds()
    if diffs.empty:
        return None
    # mode of diffs
    try:
        return int(diffs.mode().iloc[0])
    except Exception:
        return int(diffs.median())


def validate_ohlcv(df: pd.DataFrame) -> QualityReport:
    if df is None or df.empty:
        return QualityReport(0, 100.0, 0, 0.0, 0, None, notes=["empty dataframe"])

    # find datetime column
    dt_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "date" in cl or "time" in cl:
            dt_col = c
            break
    notes: List[str] = []

    if dt_col is None:
        notes.append("no datetime column detected")
        dt_col = df.columns[0]

    # standardize
    work = df.copy()
    work[dt_col] = pd.to_datetime(work[dt_col], errors="coerce")
    rows = len(work)

    # missing
    numeric_cols = [c for c in REQUIRED_COLS if c in work.columns]
    missing = work[numeric_cols].isna().sum().sum() if numeric_cols else 0
    missing_pct = float(100.0 * missing / (rows * max(1, len(numeric_cols)))) if rows else 0.0

    # duplicates by ts
    dup_ts = int(work.duplicated(subset=[dt_col]).sum())

    # simple z-score outliers on returns
    outlier_pct = 0.0
    try:
        work = work.sort_values(dt_col)
        if "close" in work.columns:
            r = work["close"].pct_change().replace([np.inf, -np.inf], np.nan).dropna()
            if len(r) > 5:
                z = (r - r.mean()) / (r.std(ddof=0) + 1e-9)
                outlier_pct = float(100.0 * (np.abs(z) > 5.0).mean())
    except Exception as e:
        notes.append(f"outlier calc failed: {e}")

    # gaps
    gap_count = 0
    tf_sec = _detect_timeframe_seconds(work, dt_col)
    try:
        if tf_sec:
            ts = pd.to_datetime(work[dt_col]).sort_values()
            diffs = ts.diff().dropna().dt.total_seconds()
            gap_count = int((diffs > 2.5 * tf_sec).sum())
    except Exception as e:
        notes.append(f"gap check failed: {e}")

    return QualityReport(
        rows=rows,
        missing_pct=round(missing_pct, 3),
        duplicate_timestamps=dup_ts,
        outlier_pct=round(outlier_pct, 3),
        gap_count=gap_count,
        timeframe_seconds=tf_sec,
        notes=notes,
    )


def detect_anomalies(df: pd.DataFrame, z_threshold: float = 5.0) -> pd.DataFrame:
    """Return subset of rows flagged as anomalies based on return z-scores and OHLC sanity checks."""
    if df is None or df.empty:
        return df
    dt_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "date" in cl or "time" in cl:
            dt_col = c
            break
    if dt_col is None:
        return pd.DataFrame(columns=list(df.columns) + ["anomaly_reason"])
    work = df.copy()
    work[dt_col] = pd.to_datetime(work[dt_col], errors="coerce")
    work = work.dropna(subset=[dt_col]).sort_values(dt_col)

    # flags
    flags = pd.Series(False, index=work.index)

    # price sanity
    if all(c in work.columns for c in REQUIRED_COLS):
        invalid = (work["high"] < work[["open", "close"]].max(axis=1)) | (work["low"] > work[["open", "close"]].min(axis=1))
        flags = flags | invalid
    # extreme returns
    if "close" in work.columns:
        r = work["close"].pct_change()
        z = (r - r.mean()) / (r.std(ddof=0) + 1e-9)
        flags = flags | (np.abs(z) > z_threshold)

    out = work.loc[flags].copy()
    out["anomaly_reason"] = "rule-based"
    return out


def cross_validate(a: pd.DataFrame, b: pd.DataFrame, tolerance: float = 0.01) -> Dict[str, Any]:
    """Cross compare two sources by aligning timestamps and measuring close-price deltas.

    Returns summary metrics and a sample of mismatches.
    """
    try:
        def _prep(x: pd.DataFrame) -> pd.DataFrame:
            x = x.copy()
            dt = None
            for c in x.columns:
                cl = str(c).lower()
                if "date" in cl or "time" in cl:
                    dt = c
                    break
            if dt is None:
                raise ValueError("no datetime column")
            x[dt] = pd.to_datetime(x[dt])
            cols = [dt, "close"] if "close" in x.columns else [dt]
            return x[cols].rename(columns={dt: "datetime", "close": "close"})

        aa = _prep(a)
        bb = _prep(b)
        merged = aa.merge(bb, on="datetime", suffixes=("_a", "_b"))
        if merged.empty or "close_a" not in merged.columns or "close_b" not in merged.columns:
            return {"aligned": 0, "mismatch_pct": None, "sample": []}
        rel_diff = (merged["close_a"] - merged["close_b"]).abs() / merged[["close_a", "close_b"]].mean(axis=1)
        mismatches = rel_diff > tolerance
        mismatch_pct = float(100.0 * mismatches.mean())
        sample = merged.loc[mismatches].head(20).to_dict(orient="records")
        return {"aligned": int(len(merged)), "mismatch_pct": round(mismatch_pct, 3), "sample": sample}
    except Exception as e:
        logger.debug(f"cross_validate error: {e}")
        return {"aligned": 0, "mismatch_pct": None, "sample": []}


def compute_time_splits(df: pd.DataFrame, train: float = 0.8, val: float = 0.1, test: float = 0.1) -> List[Tuple[str, pd.Timestamp, pd.Timestamp]]:
    """Compute time-based splits (80/10/10 by default) over the datetime span.

    Returns a list of tuples: (split_name, start_ts, end_ts).
    """
    if not np.isclose(train + val + test, 1.0):
        raise ValueError("splits must sum to 1.0")
    if df is None or df.empty:
        return []
    dt_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "date" in cl or "time" in cl:
            dt_col = c
            break
    if dt_col is None:
        raise ValueError("datetime column not found")
    s = pd.to_datetime(df[dt_col]).sort_values().dropna()
    if s.empty:
        return []
    n = len(s)
    i_train = int(n * train)
    i_val = int(n * (train + val))
    boundaries = [0, i_train, i_val, n - 1]
    # Use numpy array indexing to satisfy type checkers
    import numpy as _np
    times = s.iloc[_np.array(boundaries, dtype=int)].tolist()
    # Ensure non-overlapping windows
    return [
        ("train", times[0], times[1]),
        ("val", times[1], times[2]),
        ("test", times[2], times[3]),
    ]
