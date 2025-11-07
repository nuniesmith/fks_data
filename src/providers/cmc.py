"""CoinMarketCap provider extraction module."""
from __future__ import annotations

from typing import Any, Callable, Dict


def cmc_quotes(requester: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]], symbol: str) -> dict[str, Any]:
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
    params = {"symbol": symbol}
    headers: dict[str, str] = {}
    data = requester(url, params, headers)
    return {"raw": data}
