"""Common data type aliases and helpers for the data service.

Note: These are placeholders to avoid empty modules; replace with concrete
definitions as the data model evolves.
"""

from typing import Any, TypedDict


class Row(TypedDict, total=False):
	"""Generic row structure placeholder."""
	id: str
	data: Any


__all__ = ["Row"]

