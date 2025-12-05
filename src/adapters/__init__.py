"""Adapter registry/factory."""
from __future__ import annotations

from typing import Dict, Type

from .base import APIAdapter
from .alpha_vantage import AlphaVantageAdapter
from .binance import BinanceAdapter
from .cmc import CoinMarketCapAdapter
from .coingecko import CoinGeckoAdapter
from .eodhd import EODHDAdapter
from .finnhub import FinnhubAdapter
from .massive_futures import MassiveFuturesAdapter
from .pandas_datareader import PandasDataReaderAdapter
from .polygon import PolygonAdapter
from .tiingo import TiingoAdapter

_ADAPTERS: dict[str, type[APIAdapter]] = {
    BinanceAdapter.name: BinanceAdapter,
    PolygonAdapter.name: PolygonAdapter,
    MassiveFuturesAdapter.name: MassiveFuturesAdapter,
    EODHDAdapter.name: EODHDAdapter,
    CoinMarketCapAdapter.name: CoinMarketCapAdapter,
    CoinGeckoAdapter.name: CoinGeckoAdapter,
    AlphaVantageAdapter.name: AlphaVantageAdapter,
    FinnhubAdapter.name: FinnhubAdapter,
    TiingoAdapter.name: TiingoAdapter,
    PandasDataReaderAdapter.name: PandasDataReaderAdapter,
}


def get_adapter(name: str, **kwargs) -> APIAdapter:
    cls = _ADAPTERS.get(name.lower())
    if not cls:
        raise ValueError(f"Unknown adapter: {name}")
    return cls(**kwargs)


__all__ = ["get_adapter", "APIAdapter"]
