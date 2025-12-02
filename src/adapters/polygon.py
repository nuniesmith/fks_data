"""Polygon.io adapter using unified APIAdapter base.

Note: This adapter is for Polygon.io stocks and crypto data.
For U.S. futures data, use MassiveFuturesAdapter instead.

Supports aggregate bars endpoint.
Env vars:
  POLYGON_API_KEY or FKS_POLYGON_API_KEY for auth header.
  API keys can also be managed via the web interface (encrypted storage).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

try:
    from shared_python.exceptions import DataFetchError  # type: ignore
except (ImportError, ModuleNotFoundError):
    class DataFetchError(Exception):
        """Fallback exception when shared_python is not available."""
        pass

from .base import APIAdapter, get_env_any

logger = logging.getLogger(__name__)


class PolygonAdapter(APIAdapter):
    name = "polygon"
    base_url = "https://api.polygon.io"
    rate_limit_per_sec = 4  # conservative default
    
    # Cache TTL for market data (5 minutes)
    CACHE_TTL = 300

    def _build_request(self, **kwargs):  # noqa: D401
        ticker: str = kwargs["ticker"]
        rng: int = int(kwargs.get("range", 1))
        timespan: str = kwargs.get("timespan", "day")
        fro: str = kwargs["fro"]
        to: str = kwargs["to"]
        path = f"/v2/aggs/ticker/{ticker}/range/{rng}/{timespan}/{fro}/{to}"
        params: dict[str, Any] = {"adjusted": "true", "sort": "asc", "limit": 50000}
        api_key = get_env_any("POLYGON_API_KEY", "FKS_POLYGON_API_KEY")
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
        return self.base_url + path, params, headers

    def __init__(self, http=None, *, timeout: Optional[float] = None, enable_cache: bool = True, redis_url: Optional[str] = None):
        """Initialize Polygon adapter with optional Redis caching.
        
        Args:
            http: HTTP client (optional)
            timeout: Request timeout in seconds
            enable_cache: Enable Redis caching (default: True)
            redis_url: Redis connection URL (default: from REDIS_URL env var)
        """
        super().__init__(http, timeout=timeout)
        
        # Initialize Redis cache
        self.enable_cache = enable_cache and HAS_REDIS
        self.redis_client = None
        
        if self.enable_cache:
            try:
                redis_url = redis_url or os.getenv("REDIS_URL", "redis://:@redis:6379/0")
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                self.redis_client.ping()
                logger.info("Polygon adapter initialized with Redis cache")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
                self.redis_client = None
                self.enable_cache = False
    
    def _build_cache_key(self, **kwargs) -> str:
        """Build cache key from request parameters."""
        ticker = kwargs.get("ticker", "")
        timespan = kwargs.get("timespan", "day")
        rng = kwargs.get("range", 1)
        fro = kwargs.get("fro", "")
        to = kwargs.get("to", "")
        
        key_parts = ["polygon", ticker, timespan, str(rng)]
        if fro:
            key_parts.append(f"from_{fro}")
        if to:
            key_parts.append(f"to_{to}")
        
        return ":".join(key_parts)
    
    def fetch(self, **kwargs) -> dict[str, Any]:
        """Fetch Polygon data with Redis caching.
        
        Args:
            **kwargs: Request parameters (ticker, timespan, range, fro, to)
            
        Returns:
            Normalized data dictionary
        """
        # Check cache first
        if self.enable_cache and self.redis_client:
            cache_key = self._build_cache_key(**kwargs)
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    logger.debug(f"Cache HIT: {cache_key}")
                    return json.loads(cached_data)
                else:
                    logger.debug(f"Cache MISS: {cache_key}")
            except Exception as e:
                logger.warning(f"Cache GET error: {e}")
        
        # Fetch from API (using parent class implementation)
        try:
            result = super().fetch(**kwargs)
            
            # Store in cache
            if self.enable_cache and self.redis_client and result:
                cache_key = self._build_cache_key(**kwargs)
                try:
                    self.redis_client.setex(
                        cache_key,
                        self.CACHE_TTL,
                        json.dumps(result, default=str)
                    )
                    logger.debug(f"Cached: {cache_key} (TTL={self.CACHE_TTL}s)")
                except Exception as e:
                    logger.warning(f"Cache SET error: {e}")
            
            return result
        except Exception as e:
            logger.error(f"Error fetching from Polygon API: {e}")
            raise
    
    def _normalize(self, raw: Any, *, request_kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: D401
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        results = raw.get("results") or []
        data: list[dict[str, Any]] = []
        for item in results:
            try:
                data.append(
                    {
                        "ts": int(item.get("t", 0) // 1000),
                        "open": float(item.get("o", 0)),
                        "high": float(item.get("h", 0)),
                        "low": float(item.get("l", 0)),
                        "close": float(item.get("c", 0)),
                        "volume": float(item.get("v", 0)),
                    }
                )
            except Exception:
                continue
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["PolygonAdapter"]
