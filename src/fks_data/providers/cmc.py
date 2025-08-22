"""CoinMarketCap provider extraction module."""
from __future__ import annotations
from typing import Any, Dict, Callable


def cmc_quotes(requester: Callable[[str, Dict[str, Any], Dict[str, str]], Dict[str, Any]], symbol: str) -> Dict[str, Any]:
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
    params = {"symbol": symbol}
    headers: Dict[str, str] = {}
    data = requester(url, params, headers)
    return {"raw": data}
