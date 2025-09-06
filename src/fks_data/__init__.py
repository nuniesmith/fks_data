"""fks_data service package (lightweight install).

Avoid importing heavy runtime entrypoints on bare install to prevent missing
legacy shim module errors. Consumers that need the service startup should
explicitly import ``fks_data.main``.
"""
from .adapters import get_adapter  # noqa: F401

__all__ = ["get_adapter"]

