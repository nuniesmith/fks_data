"""Test configuration for fks_data.

Ensures the local `src` directory is importable as a package root so that
`import fks_data` style imports succeed without an editable install.
Also places the shared `shared/python/src` on sys.path (for canonical
`shared_python`) when present.
"""
from __future__ import annotations
import sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parent
SRC = ROOT / "src"
SHARED = ROOT / "shared" / "shared_python" / "src"

def _ensure(p: pathlib.Path):
    sp = str(p)
    if p.is_dir() and sp not in sys.path:
        sys.path.insert(0, sp)

_ensure(SRC)
_ensure(SHARED)
