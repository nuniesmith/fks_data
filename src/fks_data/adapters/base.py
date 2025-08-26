"""Base API adapter interfaces for external market data providers.

Week 2 scaffolding: establishes a thin, testable abstraction that:
 - Pulls configuration (API keys, timeouts) via shared `get_settings()`
 - Uses shared structured logging (`get_logger`)
 - Normalizes error handling via `DataFetchError`

Concrete adapters implement `_build_request` + `_normalize` only.
Runtime HTTP callable is injectable for deterministic tests.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional, Protocol
import os
import time
import random

try:  # shared package (symlinked) imports
    from shared_python.config import get_settings  # type: ignore
    from shared_python.logging import get_logger  # type: ignore
    from shared_python.exceptions import DataFetchError  # type: ignore
except Exception:  # pragma: no cover - fallback if path not yet wired
    from shared_python import get_settings  # type: ignore
    from shared_python.logging import get_logger  # type: ignore
    from shared_python.exceptions import DataFetchError  # type: ignore


class HTTPClient(Protocol):
    def __call__(self, url: str, params: Dict[str, Any] | None = None, headers: Dict[str, str] | None = None, timeout: float | None = None) -> Any:  # noqa: D401,E501
        ...


class APIAdapter(ABC):
    """Abstract base adapter.

    Subclasses implement provider-specific request construction and normalization.
    The public `.fetch()` method provides unified logging + exception discipline.
    """

    name: str = "base"
    base_url: str = ""
    rate_limit_per_sec: float | None = None  # basic sleep guard

    def __init__(self, http: HTTPClient | None = None, *, timeout: float | None = None):
        self._http = http or self._default_http
        # Configurable from env: FKS_API_TIMEOUT, FKS_<NAME>_TIMEOUT
        env_timeout_specific = os.getenv(f"FKS_{self.name.upper()}_TIMEOUT")
        env_timeout_global = os.getenv("FKS_API_TIMEOUT")
        self._timeout = (
            timeout
            if timeout is not None
            else float(env_timeout_specific or env_timeout_global or 10.0)
        )
        # Rate limit override: FKS_<NAME>_RPS or FKS_DEFAULT_RPS
        override_rps = os.getenv(f"FKS_{self.name.upper()}_RPS") or os.getenv("FKS_DEFAULT_RPS")
        if override_rps:
            try:
                self.rate_limit_per_sec = float(override_rps)
            except ValueError:
                pass
        # Retry config
        self._max_retries = int(os.getenv("FKS_API_MAX_RETRIES", "2"))
        self._backoff_base = float(os.getenv("FKS_API_BACKOFF_BASE", "0.3"))
        self._backoff_jitter = float(os.getenv("FKS_API_BACKOFF_JITTER", "0.25"))
        self._log = get_logger(f"fks_data.adapters.{self.name}")
        self._settings = get_settings()
        self._last_call_ts: float | None = None

    # ----------------- Public API -----------------
    def fetch(self, **kwargs) -> Dict[str, Any]:  # noqa: D401
        """Fetch provider payload; return normalized dict.

        Raises:
            DataFetchError: on network / format issues.
        """
        self._respect_rate_limit()
        try:
            url, params, headers = self._build_request(**kwargs)
            self._log.debug("request", extra={"url": url, "params": params})
            raw = self._request_with_retries(url, params, headers)
            normalized = self._normalize(raw, request_kwargs=kwargs)
            self._log.info("fetched", extra={"rows": len(normalized.get("data", [])), "status": "ok"})
            return normalized
        except DataFetchError:
            raise
        except Exception as e:  # pragma: no cover - defensive umbrella
            self._log.error("fetch_failed", extra={"error": str(e)})
            raise DataFetchError(self.name, str(e)) from e

    # ----------------- Overridables -----------------
    @abstractmethod
    def _build_request(self, **kwargs) -> tuple[str, Dict[str, Any] | None, Dict[str, str] | None]:
        """Return (url, params, headers)."""

    @abstractmethod
    def _normalize(self, raw: Any, *, request_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw provider payload -> canonical structure.

        Expected canonical dict keys:
            provider: str
            data: list[dict]
        """

    # ----------------- Helpers -----------------
    def _respect_rate_limit(self) -> None:
        if not self.rate_limit_per_sec:
            return
        now = time.time()
        if self._last_call_ts is None:
            self._last_call_ts = now
            return
        min_interval = 1.0 / max(self.rate_limit_per_sec, 1e-9)
        elapsed = now - self._last_call_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_call_ts = time.time()

    def _request_with_retries(self, url: str, params: Dict[str, Any] | None, headers: Dict[str, str] | None):  # noqa: D401
        attempt = 0
        last_err: Exception | None = None
        while attempt <= self._max_retries:
            try:
                return self._http(url, params=params, headers=headers, timeout=self._timeout)
            except Exception as e:  # broad catch to wrap network errors
                last_err = e
                if attempt == self._max_retries:
                    raise DataFetchError(self.name, f"failed after {attempt+1} attempts: {e}") from e
                sleep_for = self._backoff_base * (2 ** attempt)
                if self._backoff_jitter:
                    sleep_for += random.random() * self._backoff_jitter
                self._log.warning(
                    "retrying", extra={"attempt": attempt + 1, "max": self._max_retries + 1, "sleep": round(sleep_for, 4)}
                )
                time.sleep(sleep_for)
                attempt += 1
        # Should not reach here
        if last_err:  # pragma: no cover
            raise DataFetchError(self.name, f"unreachable retry loop termination: {last_err}") from last_err
        raise DataFetchError(self.name, "unreachable state without error")  # pragma: no cover

    # Default HTTP client using requests (lazy import to avoid hard dep if tests inject stub)
    def _default_http(self, url: str, params: Dict[str, Any] | None = None, headers: Dict[str, str] | None = None, timeout: float | None = None):  # noqa: D401,E501
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise DataFetchError(self.name, f"requests missing: {e}")
        r = requests.get(url, params=params, headers=headers, timeout=timeout or 10)
        r.raise_for_status()
        return r.json()


def get_env_any(*names: str, default: str | None = None) -> str | None:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


__all__ = ["APIAdapter", "get_env_any"]
