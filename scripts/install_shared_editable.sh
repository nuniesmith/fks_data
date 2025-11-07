#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_ROOT="$(cd "$HERE/.." && pwd)"
REPO_ROOT="$(cd "$SERVICE_ROOT/.." && pwd)"  # .../repos/fks
SHARED_DIR="$REPO_ROOT/../shared/python"

if [ ! -f "$SHARED_DIR/pyproject.toml" ]; then
  echo "shared_python not found at: $SHARED_DIR" >&2
  exit 1
fi

echo "+ Installing editable shared_python from $SHARED_DIR"
pip install -e "$SHARED_DIR"
echo "Done. Run: python scripts/test_shared_integration.py"
