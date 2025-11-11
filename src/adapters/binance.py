"""Binance adapter (Futures/Spot klines minimal) built on Week 2 scaffolding."""
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


class BinanceAdapter(APIAdapter):
    name = "binance"
    base_url = "https://fapi.binance.com"
    rate_limit_per_sec = 10  # conservative (Binance allows more, we keep low)
    
    # Cache TTL for market data (5 minutes)
    CACHE_TTL = 300

    def _build_request(self, **kwargs):  # noqa: D401
        symbol: str = kwargs.get("symbol", "BTCUSDT")
        interval: str = kwargs.get("interval", "1m")
        limit: int = int(kwargs.get("limit", 500))
        start_time = kwargs.get("start_time")
        end_time = kwargs.get("end_time")
        path = "/fapi/v1/klines"
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        # Binance public klines need no auth; placeholder for future API key usage
        headers: dict[str, str] | None = None
        return self.base_url + path, params, headers

    def __init__(self, http=None, *, timeout: Optional[float] = None, enable_cache: bool = True, redis_url: Optional[str] = None):
        """Initialize Binance adapter with optional Redis caching.
        
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
                logger.info("Binance adapter initialized with Redis cache")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
                self.redis_client = None
                self.enable_cache = False
    
    def _build_cache_key(self, **kwargs) -> str:
        """Build cache key from request parameters."""
        symbol = kwargs.get("symbol", "BTCUSDT")
        interval = kwargs.get("interval", "1m")
        limit = kwargs.get("limit", 500)
        start_time = kwargs.get("start_time")
        end_time = kwargs.get("end_time")
        
        key_parts = ["binance", symbol, interval, str(limit)]
        if start_time:
            key_parts.append(f"from_{start_time}")
        if end_time:
            key_parts.append(f"to_{end_time}")
        
        return ":".join(key_parts)
    
    def fetch(self, **kwargs) -> dict[str, Any]:
        """Fetch Binance data with Redis caching.
        
        Args:
            **kwargs: Request parameters (symbol, interval, limit, etc.)
            
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
            logger.error(f"Error fetching from Binance API: {e}")
            raise
    
    def _normalize(self, raw: Any, *, request_kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: D401
        if not isinstance(raw, list):  # Unexpected shape
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        data: list[dict[str, Any]] = []
        for item in raw:
            # Official format: [ openTime, open, high, low, close, volume, closeTime, ... ]
            try:
                data.append(
                    {
                        "ts": int(item[0] // 1000),
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                    }
                )
            except Exception:  # pragma: no cover - skip malformed row
                continue
        return {"provider": self.name, "data": data, "request": request_kwargs}


__all__ = ["BinanceAdapter"]
