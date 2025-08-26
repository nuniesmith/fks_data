"""Alpha Vantage provider extraction.

Pure data retrieval helpers (no Flask objects). Caller supplies a requester callable.
"""
from __future__ import annotations
from typing import Any, Dict, List, Callable


def alpha_daily(requester: Callable[[str, Dict[str, Any]], Dict[str, Any]], symbol: str, function: str, outputsize: str) -> Dict[str, Any]:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": function,
        "symbol": symbol,
        "outputsize": outputsize,
        "datatype": "json",
    }
    j = requester(url, params)
    key = next((k for k in j.keys() if "Time Series" in k), None)
    series = j.get(key) if key else None
    rows: List[Dict[str, Any]] = []
    if isinstance(series, dict):
        for date, vals in series.items():
            rows.append({
                "date": date,
                "open": float(vals.get("1. open", 0)),
                "high": float(vals.get("2. high", 0)),
                "low": float(vals.get("3. low", 0)),
                "close": float(vals.get("4. close", 0)),
                "adj_close": float(vals.get("5. adjusted close", vals.get("4. close", 0))),
                "volume": float(vals.get("6. volume", 0)),
            })
    return {"rows": rows}


def alpha_intraday(requester: Callable[[str, Dict[str, Any]], Dict[str, Any]], symbol: str, interval: str, outputsize: str) -> Dict[str, Any]:
    url = "https://www.alphavantage.co/query"
    params = {"function": "TIME_SERIES_INTRADAY", "symbol": symbol, "interval": interval, "outputsize": outputsize}
    j = requester(url, params)
    key_name = f"Time Series ({interval})"
    series = j.get(key_name, {})
    rows: List[Dict[str, Any]] = []
    if isinstance(series, dict):
        for ts, vals in series.items():
            rows.append({
                "time": ts,
                "open": float(vals.get("1. open", 0)),
                "high": float(vals.get("2. high", 0)),
                "low": float(vals.get("3. low", 0)),
                "close": float(vals.get("4. close", 0)),
                "volume": float(vals.get("5. volume", 0)),
            })
    return {"rows": rows}


def alpha_news(requester: Callable[[str, Dict[str, Any]], Dict[str, Any]], tickers: str, topics: str | None, time_from: str | None, time_to: str | None, limit: int) -> Dict[str, Any]:
    url = "https://www.alphavantage.co/query"
    params: Dict[str, Any] = {"function": "NEWS_SENTIMENT", "tickers": tickers, "sort": "LATEST"}
    if topics: params["topics"] = topics
    if time_from: params["time_from"] = time_from
    if time_to: params["time_to"] = time_to
    if limit: params["limit"] = max(1, min(limit, 50))
    j = requester(url, params)
    feed = j.get("feed") or []
    items: List[Dict[str, Any]] = []
    for it in feed:
        items.append({
            "title": it.get("title"),
            "time_published": it.get("time_published"),
            "source": it.get("source"),
            "url": it.get("url"),
            "summary": it.get("summary"),
            "overall_sentiment_score": it.get("overall_sentiment_score"),
            "overall_sentiment_label": it.get("overall_sentiment_label"),
            "ticker_sentiment": it.get("ticker_sentiment"),
        })
    return {"items": items}
