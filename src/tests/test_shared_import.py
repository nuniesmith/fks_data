def test_shared_import():
    # Prefer canonical package; fallback to legacy alias to retain backward compatibility.
    try:
        from shared_python import get_settings  # type: ignore
    except Exception:  # pragma: no cover
        from shared_python import get_settings  # type: ignore
    assert callable(get_settings)
