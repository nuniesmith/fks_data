"""DataManager facade (flat layout).

Provides a thin orchestration layer over adapter factory so tests can
instantiate `DataManager` and call `fetch_market_data` similar to prior
namespaced implementation.
"""
from __future__ import annotations

from typing import Any, Dict

from adapters import get_adapter  # type: ignore


class DataManager:
	def __init__(self):
		self._adapter_factory = get_adapter

	def fetch_market_data(self, provider: str, **kwargs) -> Dict[str, Any]:  # noqa: D401
		adapter = self._adapter_factory(provider)
		return adapter.fetch(**kwargs)

__all__ = ["DataManager"]
