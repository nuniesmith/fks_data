from __future__ import annotations
import sys, pathlib
root = pathlib.Path(__file__).resolve().parent

# Ensure service src is early for local package resolution
service_src = root / "src"
if service_src.is_dir():
    sp = str(service_src)
    if sp not in sys.path:
        sys.path.insert(0, sp)

# If the project depends on external shared (shared_python), avoid shadowing
local_shadow = root / "shared_python"
if local_shadow.exists():
    # Remove any already-imported shadow modules to force re-import from dependency
    for mod in list(sys.modules):
        if mod == "shared_python" or mod.startswith("shared_python.") or mod == "shared_python" or mod.startswith("shared_python."):
            sys.modules.pop(mod, None)
    # Do not add local shadow path; rely on Poetry dependency path ordering

