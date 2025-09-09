"""Package wrapper for active_assets top-level module."""
from importlib import import_module as _imp
_aa = _imp('active_assets')  # type: ignore
globals().update({k: getattr(_aa, k) for k in dir(_aa) if not k.startswith('_')})
