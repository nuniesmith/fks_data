def test_import_main():
    import importlib
    mod = importlib.import_module("fks_data.main")
    assert hasattr(mod, "main") or hasattr(mod, "run")
