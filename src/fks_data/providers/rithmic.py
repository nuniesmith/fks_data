"""Rithmic provider module (currently mock-only).

This module encapsulates the mock OHLCV logic used when real Rithmic
credentials are not configured. Real adapter integration can later
replace the placeholder function while keeping the public interface.
"""
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional

# yfinance is optional; caller passes the imported reference (or None)
def mock_ohlcv(yf_mod, symbol: str, limit: int = 500) -> Dict[str, Any]:
    """Return mock OHLCV data for a futures symbol using yfinance.

    Parameters
    ----------
    yf_mod : module or None
        The imported yfinance module; if None or download fails, returns empty set.
    symbol : str
        Futures root or symbol (e.g. 'GC').
    limit : int
        Maximum number of rows to return (most recent N).
    """
    rows: List[Dict[str, Any]] = []
    if yf_mod is not None:
        try:  # pragma: no cover - network dependent
            df = yf_mod.download(symbol, period="1y", interval="1d", progress=False)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            df = None  # type: ignore
    else:  # pragma: no cover
        df = None  # type: ignore

    if df is not None and not df.empty:  # type: ignore
        # Flatten multi-index columns if present
        cols = []
        for c in df.columns:  # type: ignore[attr-defined]
            if isinstance(c, tuple):
                cols.append("_".join([str(x) for x in c if x]))
            else:
                cols.append(str(c))
        df.columns = cols  # type: ignore[attr-defined]
        out = df.reset_index()  # type: ignore[attr-defined]
        rename_map = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        safe = {k: v for k, v in rename_map.items() if k in out.columns}
        out = out.rename(columns=safe)  # type: ignore[attr-defined]
        if "date" in out.columns:  # type: ignore[attr-defined]
            out["date"] = out["date"].astype(str)  # type: ignore[attr-defined]
        rows = out.to_dict(orient="records")  # type: ignore[attr-defined]

    if rows:
        rows = rows[-limit:]
    return {
        "meta": {"symbol": symbol, "provider": "rithmic", "source": "mock-yfinance"},
        "count": len(rows),
        "data": rows,
    }
