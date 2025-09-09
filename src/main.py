"""Data service entrypoint (flat layout)."""

import os
import sys
import base64
import hashlib

try:
    from framework.services.template import (
        start_template_service as _framework_start_template_service,
    )  # type: ignore
except Exception:  # pragma: no cover - framework not present in minimal test env
    def _framework_start_template_service(*args, **kwargs):  # type: ignore
        # Provide a tiny fallback so importing the module in tests doesn't explode.
        print("[fks_data.main] framework.services.template missing - fallback noop service start")

# Module-level optional cryptography import so helpers are patchable/testable
try:  # pragma: no cover - import guard
    from cryptography.fernet import Fernet  # type: ignore
except Exception:  # pragma: no cover
    Fernet = None  # type: ignore


def _get_fernet():
    """Return a Fernet instance derived from DATA_KEYS_SECRET/FKS_KEYS_SECRET.

    Kept outside the closure so tests can monkeypatch encryption helpers to
    force error paths (previously closure-local making fault injection hard).
    """
    secret = os.getenv("DATA_KEYS_SECRET") or os.getenv("FKS_KEYS_SECRET")
    if not secret or Fernet is None:
        return None
    try:
        if secret.startswith("fernet:"):
            key_b64 = secret.split(":", 1)[1].strip().encode()
        else:
            digest = hashlib.sha256(secret.encode()).digest()
            key_b64 = base64.urlsafe_b64encode(digest)
        return Fernet(key_b64)
    except Exception:  # pragma: no cover
        return None


def _encrypt_value(val):  # pragma: no cover - exercised indirectly
    if not val:
        return val
    f = _get_fernet()
    if not f:
        return val
    try:
        return f.encrypt(str(val).encode()).decode()
    except Exception:
        return val


def _decrypt_value(val, enc):  # pragma: no cover - exercised indirectly
    if not val:
        return val
    if not enc:
        return val
    f = _get_fernet()
    if not f:
        return None
    try:
        return f.decrypt(str(val).encode()).decode()
    except Exception:
        return None

# Reconstituted minimal custom endpoint builder
def _custom_endpoints():  # noqa: C901
    import re
    import time
    from typing import Any, Dict, Optional
    from flask import request, jsonify  # type: ignore

    try:
        from cryptography.fernet import Fernet  # type: ignore
    except Exception:  # pragma: no cover
        Fernet = None  # type: ignore
    try:
        import yfinance as yf  # type: ignore
    except Exception:  # pragma: no cover
        yf = None  # type: ignore

    try:
        import fcntl  # type: ignore
    except Exception:  # pragma: no cover
        fcntl = None  # type: ignore

    # Optional external libs
    try:
        import requests  # type: ignore
    except Exception:  # pragma: no cover
        requests = None  # type: ignore

    # Provider modules (optional)
    try:
        from services.data.providers import rithmic as _prov_rithmic  # type: ignore
    except Exception:  # pragma: no cover
        _prov_rithmic = None  # type: ignore
    try:
        from services.data.providers import alpha as _prov_alpha  # type: ignore
    except Exception:  # pragma: no cover
        _prov_alpha = None  # type: ignore
    try:
        from services.data.providers import polygon as _prov_polygon  # type: ignore
    except Exception:  # pragma: no cover
        _prov_polygon = None  # type: ignore
    try:
        from services.data.providers import binance as _prov_binance  # type: ignore
    except Exception:  # pragma: no cover
        _prov_binance = None  # type: ignore

    # Basic helpers -------------------------------------------------
    def _require_auth():
        token_required = (
            os.getenv("FKS_DATA_ADMIN_TOKEN")
            or os.getenv("DATA_ADMIN_TOKEN")
            or os.getenv("ADMIN_TOKEN")
        )
        if not token_required:
            return None
        auth_header = request.headers.get("Authorization", "").strip()
        candidate = None
        if auth_header.startswith("Bearer "):
            candidate = auth_header[7:].strip()
        elif auth_header:
            candidate = auth_header
        candidate = candidate or request.headers.get("X-API-Key") or request.headers.get("X-Admin-Token") or request.args.get("api_key")
        if candidate != token_required:
            return jsonify({"ok": False, "error": "unauthorized", "code": "unauthorized"}), 401
        return None

    def _error(message: str, status: int = 400, code: Optional[str] = None, **extra):
        payload = {"ok": False, "error": message}
        if code:
            payload["code"] = code
        if extra:
            payload.update(extra)
        return jsonify(payload), status

    def _ok(payload: Dict[str, Any]):
        if not isinstance(payload, dict):
            return jsonify({"ok": True, "data": payload})
        if "ok" not in payload:
            payload = {"ok": True, **payload}
        return jsonify(payload)

    def _success(data=None, **meta):
        p = {"ok": True, **meta}
        if data is not None:
            p["data"] = data
        return jsonify(p)

    # Cache ---------------------------------------------------------
    from threading import Lock as _Lock
    _CACHE: Dict[str, tuple[float, Any]] = {}
    _CACHE_LOCK = _Lock()
    _DEFAULT_TTL = float(os.getenv("DATA_CACHE_DEFAULT_TTL", "30"))

    def _cache_key(name: str) -> str:
        try:
            return name + "|" + "&".join(f"{k}={v}" for k, v in sorted(request.args.items()))
        except Exception:
            return name

    def _cache_get(name: str):
        if request.method != "GET":
            return None
        key = _cache_key(name)
        now = time.time()
        with _CACHE_LOCK:
            ent = _CACHE.get(key)
            if not ent:
                return None
            exp, val = ent
            if exp < now:
                _CACHE.pop(key, None)
                return None
            return val

    def _cache_set(name: str, value, ttl: Optional[float] = None):
        if request.method != "GET":
            return
        key = _cache_key(name)
        with _CACHE_LOCK:
            _CACHE[key] = (time.time() + (ttl if ttl is not None else _DEFAULT_TTL), value)

    # Key storage & encryption -------------------------------------
    def _load_saved_keys_file() -> Dict[str, Any]:
        try:
            base_dir = os.getenv("FKS_DATA_DIR") or os.path.join(os.getcwd(), "data", "managed")
            os.makedirs(base_dir, exist_ok=True)
            path = os.path.join(base_dir, "provider_keys.json")
            if os.path.exists(path):
                import json as _json
                with open(path, "r", encoding="utf-8") as f:
                    return _json.load(f) or {}
        except Exception:  # pragma: no cover
            pass
        return {}

    def _locked_file_write(path: str, writer):
        if fcntl is None:
            with open(path, "w", encoding="utf-8") as fh:
                writer(fh)
            return
        with open(path, "a+", encoding="utf-8") as fh:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX)
            except Exception:  # pragma: no cover
                pass
            fh.seek(0); fh.truncate(); writer(fh)
            try:
                fcntl.flock(fh, fcntl.LOCK_UN)
            except Exception:  # pragma: no cover
                pass

    # Encryption helpers now module-level (_get_fernet/_encrypt_value/_decrypt_value)

    def _get_saved_key(provider: str) -> Optional[Dict[str, Any]]:
        data = _load_saved_keys_file()
        v = data.get(provider)
        if isinstance(v, dict):
            if v.get("enc"):
                api_key = _decrypt_value(v.get("api_key"), True)
                secret = _decrypt_value(v.get("secret"), True) if v.get("secret") else None
                return {"api_key": api_key, **({"secret": secret} if secret else {})}
            return {"api_key": v.get("api_key"), **({"secret": v.get("secret")} if v.get("secret") else {})}
        return None

    def _mask_short(s: Optional[str]):
        if not s:
            return None
        return s[:3] + "***" + (s[-3:] if len(s) > 6 else "")

    # Providers meta ------------------------------------------------
    def _providers_info():
        out = [
            {"name": "rithmic", "mock": True, "status": "mock"},
            {"name": "alpha", "status": "available", "daily": True, "intraday": True, "news": True},
            {"name": "polygon", "status": "available", "aggs": True},
            {"name": "binance", "status": "available", "klines": True},
        ]
        return _ok({"providers": out, "count": len(out)})

    def _providers_keys():
        # Only list existence, never raw secret
        providers = ["rithmic", "alpha", "polygon", "binance"]
        out = {}
        for p in providers:
            sk = _get_saved_key(p)
            out[p] = {"exists": bool(sk), "masked": _mask_short((sk or {}).get("api_key"))}
        return _ok({"providers": out})

    # Rithmic mock --------------------------------------------------
    def futures_rithmic_ohlcv():
        symbol = request.args.get("symbol", "GC")
        try:
            limit = int(request.args.get("limit", "500"))
        except Exception:
            limit = 500
        mock = str(os.getenv("RITHMIC_MOCK", "1")).lower() in ("1", "true", "yes")
        if not mock:
            return _error("Mock mode only in this environment", 400, code="no_mock")
        if _prov_rithmic is None:
            return _error("rithmic provider module unavailable", 500, code="rithmic_module_missing")
        try:
            res = _prov_rithmic.mock_ohlcv(yf, symbol, limit)  # type: ignore[attr-defined]
            # Test expects top-level ok and nested data dict, so embed provider response
            return _ok({"data": res})
        except Exception as e:  # pragma: no cover
            return _error(str(e), 500, code="mock_failed")

    # Alpha endpoints -------------------------------------------------
    def _alpha_daily():
        if _prov_alpha is None or requests is None:
            return _error("alpha provider unavailable", 500, code="alpha_unavailable")
        symbol = request.args.get("symbol", "AAPL")
        func = request.args.get("function", "TIME_SERIES_DAILY_ADJUSTED")
        outputsize = request.args.get("outputsize", "compact")
        api_key = (_get_saved_key("alpha") or {}).get("api_key") or os.getenv("ALPHA_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY")
        if not api_key:
            return _error("alpha api key missing", 400, code="missing_key")
        def _req(url, params):  # type: ignore[override]
            params["apikey"] = api_key
            if requests is None:  # defensive
                raise RuntimeError("requests library unavailable")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        try:
            data = _prov_alpha.alpha_daily(_req, symbol, func, outputsize)
            return _ok(data)
        except Exception as e:  # pragma: no cover
            return _error(str(e), 500, code="alpha_failed")

    def _alpha_intraday():
        if _prov_alpha is None or requests is None:
            return _error("alpha provider unavailable", 500, code="alpha_unavailable")
        symbol = request.args.get("symbol", "AAPL")
        interval = request.args.get("interval", "5min")
        outputsize = request.args.get("outputsize", "compact")
        api_key = (_get_saved_key("alpha") or {}).get("api_key") or os.getenv("ALPHA_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY")
        if not api_key:
            return _error("alpha api key missing", 400, code="missing_key")
        def _req(url, params):  # type: ignore[override]
            params["apikey"] = api_key
            if requests is None:
                raise RuntimeError("requests library unavailable")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        try:
            data = _prov_alpha.alpha_intraday(_req, symbol, interval, outputsize)
            return _ok(data)
        except Exception as e:  # pragma: no cover
            return _error(str(e), 500, code="alpha_failed")

    def _alpha_news():
        if _prov_alpha is None or requests is None:
            return _error("alpha provider unavailable", 500, code="alpha_unavailable")
        tickers = request.args.get("tickers", "AAPL,MSFT")
        topics = request.args.get("topics")
        time_from = request.args.get("time_from")
        time_to = request.args.get("time_to")
        limit = int(request.args.get("limit", "25"))
        api_key = (_get_saved_key("alpha") or {}).get("api_key") or os.getenv("ALPHA_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY")
        if not api_key:
            return _error("alpha api key missing", 400, code="missing_key")
        def _req(url, params):  # type: ignore[override]
            params["apikey"] = api_key
            if requests is None:
                raise RuntimeError("requests library unavailable")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        try:
            data = _prov_alpha.alpha_news(_req, tickers, topics, time_from, time_to, limit)
            return _ok(data)
        except Exception as e:  # pragma: no cover
            return _error(str(e), 500, code="alpha_failed")

    # Polygon endpoint ----------------------------------------------
    def _polygon_aggs():
        if _prov_polygon is None or requests is None:
            return _error("polygon provider unavailable", 500, code="polygon_unavailable")
        ticker = request.args.get("ticker", "AAPL")
        rng = request.args.get("range", "1")
        timespan = request.args.get("timespan", "day")
        fro = request.args.get("from", "2024-01-01")
        to = request.args.get("to", "2024-01-02")
        api_key = (_get_saved_key("polygon") or {}).get("api_key") or os.getenv("POLYGON_API_KEY")
        if not api_key:
            return _error("polygon api key missing", 400, code="missing_key")
        def _req(url, params):  # type: ignore[override]
            params["apikey"] = api_key
            if requests is None:
                raise RuntimeError("requests library unavailable")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        try:
            data = _prov_polygon.polygon_aggs(_req, ticker, rng, timespan, fro, to)
            return _ok(data)
        except Exception as e:  # pragma: no cover
            return _error(str(e), 500, code="polygon_failed")

    # Binance endpoint ----------------------------------------------
    def _binance_klines():
        if _prov_binance is None or requests is None:
            return _error("binance provider unavailable", 500, code="binance_unavailable")
        symbol = request.args.get("symbol", "BTCUSDT")
        interval = request.args.get("interval", "1m")
        limit = int(request.args.get("limit", "500"))
        start = request.args.get("start_time")
        end = request.args.get("end_time")
        def _req(url, params):  # type: ignore[override]
            if requests is None:
                raise RuntimeError("requests library unavailable")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        try:
            data = _prov_binance.binance_klines(_req, symbol, interval, limit, start, end)
            return _ok(data)
        except Exception as e:  # pragma: no cover
            return _error(str(e), 500, code="binance_failed")

    # Config endpoints ----------------------------------------------
    def _mask_sensitive(k: str, v: str):
        if not isinstance(v, str):
            return v
        if re.search(r"(key|secret|token|password)", k, re.IGNORECASE):
            h = hashlib.sha256(v.encode("utf-8")).hexdigest()[:8]
            return f"***{h}"
        return v

    def _config_get():
        cached = _cache_get("merged_config")
        if cached is not None:
            return cached
        raw = {k: v for k, v in os.environ.items() if k.startswith("FKS_") or k.endswith("_API_KEY")}
        masked = {k: _mask_sensitive(k, v) for k, v in raw.items()}
        resp = _ok({"overrides": masked})
        _cache_set("merged_config", resp, ttl=5)
        return resp

    _runtime_overrides: Dict[str, str] = {}

    def _config_set():
        auth_fail = _require_auth()
        if auth_fail:
            return auth_fail
        body = request.get_json(force=True, silent=True) or {}
        changed = {}
        for k, v in body.items():
            if not isinstance(k, str):
                continue
            if not isinstance(v, (str, int, float, bool)) and v is not None:
                continue
            sval = str(v) if v is not None else ""
            _runtime_overrides[k] = sval
            os.environ[k] = sval
            changed[k] = sval
        _cache_set("merged_config", None, ttl=1)
        return _ok({"updated": changed, "count": len(changed)})

    # Provider key management --------------------------------------
    def _save_provider_key():  # POST
        auth_fail = _require_auth()
        if auth_fail:
            return auth_fail
        provider = (request.view_args or {}).get("provider") if request.view_args else None
        if not provider:
            return _error("Missing provider", 400, code="missing_provider")
        try:
            payload = request.get_json(force=True, silent=True) or {}
            api_key = (payload.get("api_key") or payload.get("apikey") or "").strip()
            secret = (payload.get("secret") or payload.get("api_secret") or None)
            if not api_key:
                return _error("api_key required", 400, code="missing_api_key")
            base_dir = os.getenv("FKS_DATA_DIR") or os.path.join(os.getcwd(), "data", "managed")
            os.makedirs(base_dir, exist_ok=True)
            path = os.path.join(base_dir, "provider_keys.json")
            import json as _json
            data = _load_saved_keys_file()
            f = _get_fernet()
            if f is not None:
                data[provider] = {"api_key": _encrypt_value(api_key), **({"secret": _encrypt_value(secret)} if secret else {}), "enc": True}
            else:
                data[provider] = {"api_key": api_key, **({"secret": secret} if secret else {}), "enc": False}
            def _write(fh):
                _json.dump(data, fh, indent=2)
            _locked_file_write(path, _write)
            return _success(provider=provider)
        except Exception as e:
            return _error(str(e), 500, code="save_failed")

    def _get_provider_key():  # GET
        auth_fail = _require_auth()
        if auth_fail:
            return auth_fail
        provider = (request.view_args or {}).get("provider") if request.view_args else None
        if not provider:
            return _error("Missing provider", 400, code="missing_provider")
        key = (_get_saved_key(provider) or {}).get("api_key")
        return _success(provider=provider, exists=bool(key), masked=_mask_short(key))

    def _provider_key_handler(provider=None):  # combined
        return _save_provider_key() if request.method == "POST" else _get_provider_key()

    # Route map -----------------------------------------------------
    routes: Dict[str, Any] = {
        "/providers": _providers_info,
        "/providers/keys": _providers_keys,
        "/futures/rithmic/ohlcv": futures_rithmic_ohlcv,
        "/config": _config_get,
        "/config/set": {"handler": _config_set, "methods": ["POST"]},
        "/providers/<provider>/key": {"handler": _provider_key_handler, "methods": ["GET", "POST"]},
    # Newly reintroduced provider endpoints
    "/providers/alpha/daily": _alpha_daily,
    "/providers/alpha/intraday": _alpha_intraday,
    "/providers/alpha/news": _alpha_news,
    "/providers/polygon/aggs": _polygon_aggs,
    "/crypto/binance/klines": _binance_klines,
    }

    return routes


def start_template_service(service_name: str | None = None, service_port: int | None = None):
    """Wrapper so the runner can call this and still get our custom endpoints."""
    if service_name:
        os.environ["DATA_SERVICE_NAME"] = str(service_name)
    if service_port is not None:
        os.environ["DATA_SERVICE_PORT"] = str(service_port)

    name = os.getenv("DATA_SERVICE_NAME", "data")
    port = int(os.getenv("DATA_SERVICE_PORT", "9001"))
    _framework_start_template_service(
        service_name=name, service_port=port, custom_endpoints=_custom_endpoints()
    )


def main():
    # Set the service name and port from environment variables or defaults
    service_name = os.getenv("DATA_SERVICE_NAME", "data")
    port = os.getenv("DATA_SERVICE_PORT", "9001")

    # Run DB migrations unless disabled
    if os.getenv("FKS_SKIP_MIGRATIONS", "0").lower() not in ("1", "true", "yes"):
        try:
            from scripts.run_migrations import run as run_migrations  # type: ignore
            run_migrations()
        except Exception as e:  # pragma: no cover
            print(f"[fks_data.main] migration step skipped due to error: {e}")

    # Log the service startup
    print(f"Starting {service_name} service on port {port}")

    # Start the service using the template
    start_template_service(service_name=service_name, service_port=int(port))


if __name__ == "__main__":
    sys.exit(main())
