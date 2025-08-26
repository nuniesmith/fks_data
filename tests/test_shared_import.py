def test_shared_import():
    from shared_python import get_settings  # type: ignore
    assert get_settings is not None
