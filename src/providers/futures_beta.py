"""Futures Beta REST proxy helper module.

Abstracts forwarding logic so the Flask route layer stays thin.
"""
from __future__ import annotations
from typing import Any, Dict, Optional


def build_url(base: str, version: str, path_suffix: str) -> str:
    return base.rstrip('/') + f"/futures/{version}" + path_suffix


def forward(requests_mod, request, base: Optional[str], version: Optional[str], api_key: Optional[str],
            path_suffix: str, extra_params: Optional[Dict[str, Any]] = None):
    if requests_mod is None:
        return {"ok": False, "error": "requests not available"}, 500
    if not base:
        return {"ok": False, "error": "Set FUTURES_BETA_REST_URL"}, 400
    if not version:
        return {"ok": False, "error": "Set FUTURES_BETA_VERSION or provide ?version=vX"}, 400
    url = build_url(base, version, path_suffix)
    params = dict(request.args)
    params.pop("version", None)
    if extra_params:
        params.update(extra_params)
    if api_key and "apiKey" not in params:
        params["apiKey"] = api_key
    try:
        r = requests_mod.get(url, params=params, timeout=20)
        r.raise_for_status()
        out = r.json()
        if isinstance(out, dict) and "ok" not in out:
            out["ok"] = True
        return out, 200
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e), "url": url}, 502
