"""
Dataset splitting helpers: create 80/10/10 time-based splits from managed CSVs.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from loguru import logger

from .validation import compute_time_splits
from .active_assets import csv_path_for


def _dt_col(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        cl = str(c).lower()
        if "date" in cl or "time" in cl:
            return c
    return None


def split_managed_csv(source: str, symbol: str, interval: str, out_dir: Optional[Path] = None) -> List[Path]:
    src = csv_path_for(source, symbol, interval)
    if not src.exists():
        return []
    df = pd.read_csv(src)
    dcol = _dt_col(df)
    if dcol is None:
        return []
    df[dcol] = pd.to_datetime(df[dcol])
    df = df.sort_values(dcol)
    splits = compute_time_splits(df)

    if out_dir is None:
        out_dir = src.parent / "splits"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_paths: List[Path] = []
    for name, a, b in splits:
        part = df[(df[dcol] >= a) & (df[dcol] <= b)].copy()
        p = out_dir / f"{symbol.replace('/', '-')}_{interval}_{name}.csv"
        part.to_csv(p, index=False)
        out_paths.append(p)
    logger.info(f"Wrote splits for {source}:{symbol}:{interval} -> {len(out_paths)} files")
    return out_paths
