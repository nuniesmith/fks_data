"""Adapter registry/factory."""
from __future__ import annotations

from typing import Dict, Type

from .base import APIAdapter
from .binance import BinanceAdapter
from .eodhd import EODHDAdapter
from .polygon import PolygonAdapter

_ADAPTERS: dict[str, type[APIAdapter]] = {
    BinanceAdapter.name: BinanceAdapter,
    PolygonAdapter.name: PolygonAdapter,
    EODHDAdapter.name: EODHDAdapter,
}


def get_adapter(name: str, **kwargs) -> APIAdapter:
    cls = _ADAPTERS.get(name.lower())
    if not cls:
        raise ValueError(f"Unknown adapter: {name}")
    return cls(**kwargs)


__all__ = ["get_adapter", "APIAdapter"]
