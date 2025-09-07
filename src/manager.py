"""Package wrapper re-exporting top-level manager module.

Allows imports like ``fks_data.manager`` when the canonical implementation
resides as a top-level module (manager.py) shipped for historical reasons.
"""
from importlib import import_module as _imp

_mgr = _imp('manager')  # type: ignore
globals().update({k: getattr(_mgr, k) for k in dir(_mgr) if not k.startswith('_')})
