"""fks_data service package.

Exports entrypoints plus Week 2 adapter factory for external API connections.
"""
from .main import main, start_template_service  # re-export entry point
from .adapters import get_adapter  # noqa: F401

__all__ = ["main", "start_template_service", "get_adapter"]

