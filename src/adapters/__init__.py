"""Adapter registry/factory."""
from __future__ import annotations

from typing import Dict, Type
from .base import APIAdapter
from .binance import BinanceAdapter
from .polygon import PolygonAdapter

_ADAPTERS: Dict[str, Type[APIAdapter]] = {
    BinanceAdapter.name: BinanceAdapter,
    PolygonAdapter.name: PolygonAdapter,
}


def get_adapter(name: str, **kwargs) -> APIAdapter:
    cls = _ADAPTERS.get(name.lower())
    if not cls:
        raise ValueError(f"Unknown adapter: {name}")
    return cls(**kwargs)


__all__ = ["get_adapter", "APIAdapter"]
