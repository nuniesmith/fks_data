"""Yahoo Finance (yfinance) provider helpers."""
from __future__ import annotations
from typing import Any, Dict, List, Optional


def sample_prices(yf, symbol: str, period: str, interval: str, max_rows: int = 200) -> Dict[str, Any]:
    try:
        data = yf.download(symbol, period=period, interval=interval, progress=False)
    except Exception:
        data = None
    records: List[Dict[str, Any]] = []
    count = 0
    if data is not None and hasattr(data, 'columns') and 'Close' in data.columns:
        closes = data['Close'].dropna().tail(max_rows).reset_index().rename(columns={'Date': 'date', 'Close': 'close'})
        try:
            closes['date'] = closes['date'].astype(str)
        except Exception:
            pass
        count = int(len(closes))
        records = closes.to_dict(orient='records')  # type: ignore
    return {"count": count, "records": records}


def daily_ohlcv(yf, symbol: str, start: Optional[str], end: Optional[str], period: str) -> Dict[str, Any]:
    try:
        if start or end:
            df = yf.download(symbol, start=start, end=end, interval="1d", progress=False)
        else:
            df = yf.download(symbol, period=period, interval="1d", progress=False)
    except Exception:
        df = None
    if df is None or getattr(df, 'empty', True):
        return {"count": 0, "records": []}
    out = df.reset_index().rename(columns={
        'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'
    })
    if 'date' in out.columns:
        try:
            out['date'] = out['date'].astype(str)
        except Exception:
            pass
    recs = out.to_dict(orient='records')  # type: ignore
    return {"count": len(recs), "records": recs}


def crypto_ohlcv(yf, symbol: str, interval: str, period: str, start: Optional[str], end: Optional[str]) -> Dict[str, Any]:
    tf_map = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"60m","1d":"1d"}
    yf_interval = tf_map.get(interval, interval)
    try:
        if start or end:
            df = yf.download(symbol, start=start, end=end, interval=yf_interval, progress=False)
        else:
            df = yf.download(symbol, period=period, interval=yf_interval, progress=False)
    except Exception:
        df = None
    if df is None or getattr(df, 'empty', True):
        return {"count": 0, "records": []}
    out = df.reset_index().rename(columns={
        'Date': 'time', 'Datetime': 'time', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'
    })
    if 'time' in out.columns:
        try:
            out['time'] = (out['time'].astype('int64') // 10**9).astype(int)
        except Exception:
            try:
                out['time'] = out['time'].astype(str)
            except Exception:
                pass
    recs = out.to_dict(orient='records')  # type: ignore
    return {"count": len(recs), "records": recs}
