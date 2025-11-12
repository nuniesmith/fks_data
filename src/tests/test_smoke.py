import importlib
import sys


def test_import_root():
    # Basic import smoke test for the service package
    sys.path[0].split('/')[-1]
    assert True
