"""Massive.com Futures adapter using unified APIAdapter base.

Note: Massive.com is the new name for Polygon.io's futures division.
This adapter is specifically for U.S. futures market data (CME, CBOT, COMEX, NYMEX).
For stocks/crypto data, use the PolygonAdapter instead.

Supports futures contracts, products, schedules, aggregates (OHLC), trades, quotes,
market status, and exchanges endpoints.

Env vars:
  MASSIVE_API_KEY or FKS_MASSIVE_API_KEY or POLYGON_API_KEY for auth header.
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


class MassiveFuturesAdapter(APIAdapter):
    """Adapter for Massive.com Futures REST API.
    
    Supports:
    - Contracts: /futures/vX/contracts, /futures/vX/contracts/{ticker}
    - Products: /futures/vX/products, /futures/vX/products/{product_code}
    - Schedules: /futures/vX/schedules, /futures/vX/products/{product_code}/schedules
    - Aggregates: /futures/vX/aggs/{ticker}
    - Trades: /futures/vX/trades/{ticker}
    - Quotes: /futures/vX/quotes/{ticker}
    - Market Status: /futures/vX/market-status
    - Exchanges: /futures/vX/exchanges
    """
    name = "massive_futures"
    base_url = "https://api.massive.com"
    rate_limit_per_sec = 4  # conservative default
    
    # Cache TTL for market data (5 minutes)
    CACHE_TTL = 300

    def _build_request(self, **kwargs):  # noqa: D401
        """Build request URL, params, and headers based on endpoint type.
        
        API key resolution order:
        1. Environment variables (MASSIVE_API_KEY, FKS_MASSIVE_API_KEY, POLYGON_API_KEY)
        2. Database-stored keys (via web interface) - TODO: implement retrieval from web service
        """
        endpoint_type = kwargs.get("endpoint_type", "aggs")
        api_key = get_env_any("MASSIVE_API_KEY", "FKS_MASSIVE_API_KEY", "POLYGON_API_KEY")
        
        # TODO: Add support for retrieving API keys from web service database
        # if not api_key:
        #     api_key = self._get_api_key_from_db("massive_futures")
        
        if not api_key:
            raise DataFetchError(
                self.name, 
                "API key required. Set MASSIVE_API_KEY, FKS_MASSIVE_API_KEY, or POLYGON_API_KEY. "
                "Alternatively, add the key via the web interface (Settings > API Keys)."
            )
        
        params: dict[str, Any] = {"apiKey": api_key}
        headers: dict[str, str] | None = None
        
        # Route to appropriate endpoint
        if endpoint_type == "contracts":
            # GET /futures/vX/contracts
            path = "/futures/vX/contracts"
            # Optional params: product_code, first_trade_date, last_trade_date, as_of, active, type, limit, sort
            for param in ["product_code", "first_trade_date", "last_trade_date", "as_of", "active", "type", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "contract":
            # GET /futures/vX/contracts/{ticker}
            ticker = kwargs.get("ticker")
            if not ticker:
                raise DataFetchError(self.name, "ticker required for contract endpoint")
            path = f"/futures/vX/contracts/{ticker}"
            if "as_of" in kwargs:
                params["as_of"] = kwargs["as_of"]
        
        elif endpoint_type == "products":
            # GET /futures/vX/products
            path = "/futures/vX/products"
            for param in ["name", "as_of", "trading_venue", "sector", "sub_sector", 
                         "asset_class", "asset_sub_class", "type", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "product":
            # GET /futures/vX/products/{product_code}
            product_code = kwargs.get("product_code")
            if not product_code:
                raise DataFetchError(self.name, "product_code required for product endpoint")
            path = f"/futures/vX/products/{product_code}"
            for param in ["type", "as_of"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "schedules":
            # GET /futures/vX/schedules
            path = "/futures/vX/schedules"
            for param in ["session_end_date", "trading_venue", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "product_schedules":
            # GET /futures/vX/products/{product_code}/schedules
            product_code = kwargs.get("product_code")
            if not product_code:
                raise DataFetchError(self.name, "product_code required for product_schedules endpoint")
            path = f"/futures/vX/products/{product_code}/schedules"
            for param in ["session_end_date", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "aggs":
            # GET /futures/vX/aggs/{ticker}
            ticker = kwargs.get("ticker")
            if not ticker:
                raise DataFetchError(self.name, "ticker required for aggs endpoint")
            path = f"/futures/vX/aggs/{ticker}"
            # Required: resolution
            if "resolution" in kwargs:
                params["resolution"] = kwargs["resolution"]
            # Optional: window_start, window_start.gte, window_start.gt, window_start.lte, window_start.lt, limit, sort
            for param in ["window_start", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
            # Handle window_start comparison operators
            for op in ["gte", "gt", "lte", "lt"]:
                key = f"window_start.{op}"
                if key in kwargs:
                    params[key] = kwargs[key]
        
        elif endpoint_type == "trades":
            # GET /futures/vX/trades/{ticker}
            ticker = kwargs.get("ticker")
            if not ticker:
                raise DataFetchError(self.name, "ticker required for trades endpoint")
            path = f"/futures/vX/trades/{ticker}"
            for param in ["timestamp", "session_end_date", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "quotes":
            # GET /futures/vX/quotes/{ticker}
            ticker = kwargs.get("ticker")
            if not ticker:
                raise DataFetchError(self.name, "ticker required for quotes endpoint")
            path = f"/futures/vX/quotes/{ticker}"
            for param in ["timestamp", "session_end_date", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "market_status":
            # GET /futures/vX/market-status
            path = "/futures/vX/market-status"
            for param in ["product_code", "limit", "sort"]:
                if param in kwargs:
                    params[param] = kwargs[param]
        
        elif endpoint_type == "exchanges":
            # GET /futures/vX/exchanges
            path = "/futures/vX/exchanges"
            if "limit" in kwargs:
                params["limit"] = kwargs["limit"]
        
        else:
            raise DataFetchError(self.name, f"Unknown endpoint_type: {endpoint_type}")
        
        return self.base_url + path, params, headers

    def __init__(self, http=None, *, timeout: Optional[float] = None, enable_cache: bool = True, redis_url: Optional[str] = None):
        """Initialize Massive Futures adapter with optional Redis caching.
        
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
                logger.info("Massive Futures adapter initialized with Redis cache")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
                self.redis_client = None
                self.enable_cache = False
    
    def _build_cache_key(self, **kwargs) -> str:
        """Build cache key from request parameters."""
        endpoint_type = kwargs.get("endpoint_type", "aggs")
        key_parts = ["massive_futures", endpoint_type]
        
        # Add identifying parameters based on endpoint type
        if endpoint_type in ["contract", "aggs", "trades", "quotes"]:
            if "ticker" in kwargs:
                key_parts.append(kwargs["ticker"])
        elif endpoint_type in ["product", "product_schedules"]:
            if "product_code" in kwargs:
                key_parts.append(kwargs["product_code"])
        
        # Add other relevant params
        for param in ["resolution", "window_start", "as_of", "session_end_date"]:
            if param in kwargs:
                key_parts.append(f"{param}_{kwargs[param]}")
        
        return ":".join(key_parts)
    
    def fetch(self, **kwargs) -> dict[str, Any]:
        """Fetch Massive Futures data with Redis caching.
        
        Args:
            **kwargs: Request parameters including endpoint_type and endpoint-specific params
            
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
            logger.error(f"Error fetching from Massive Futures API: {e}")
            raise
    
    def _normalize(self, raw: Any, *, request_kwargs: dict[str, Any]) -> dict[str, Any]:  # noqa: D401
        """Normalize Massive Futures API response to canonical format."""
        if not isinstance(raw, dict):
            raise DataFetchError(self.name, f"unexpected payload type: {type(raw)}")
        
        endpoint_type = request_kwargs.get("endpoint_type", "aggs")
        results = raw.get("results") or []
        
        # Handle different endpoint types
        if endpoint_type == "aggs":
            # Aggregate bars (OHLC)
            data: list[dict[str, Any]] = []
            for item in results:
                try:
                    # Convert nanosecond timestamp to seconds
                    window_start = item.get("window_start", 0)
                    if isinstance(window_start, int) and window_start > 1e15:
                        window_start = window_start // 1_000_000_000
                    
                    data.append({
                        "ts": int(window_start),
                        "open": float(item.get("open", 0)),
                        "high": float(item.get("high", 0)),
                        "low": float(item.get("low", 0)),
                        "close": float(item.get("close", 0)),
                        "volume": int(item.get("volume", 0)),
                        "transactions": int(item.get("transactions", 0)),
                        "dollar_volume": float(item.get("dollar_volume", 0)),
                        "settlement_price": float(item.get("settlement_price", 0)) if item.get("settlement_price") else None,
                        "session_end_date": item.get("session_end_date"),
                        "ticker": item.get("ticker"),
                    })
                except Exception as e:
                    logger.warning(f"Error normalizing agg item: {e}")
                    continue
        
        elif endpoint_type == "trades":
            # Trades
            data: list[dict[str, Any]] = []
            for item in results:
                try:
                    timestamp = item.get("timestamp", 0)
                    if isinstance(timestamp, int) and timestamp > 1e15:
                        timestamp = timestamp // 1_000_000_000
                    
                    data.append({
                        "ts": int(timestamp),
                        "price": float(item.get("price", 0)),
                        "size": int(item.get("size", 0)),
                        "ticker": item.get("ticker"),
                        "session_end_date": item.get("session_end_date"),
                    })
                except Exception as e:
                    logger.warning(f"Error normalizing trade item: {e}")
                    continue
        
        elif endpoint_type == "quotes":
            # Quotes
            data: list[dict[str, Any]] = []
            for item in results:
                try:
                    timestamp = item.get("timestamp", 0)
                    if isinstance(timestamp, int) and timestamp > 1e15:
                        timestamp = timestamp // 1_000_000_000
                    
                    data.append({
                        "ts": int(timestamp),
                        "bid_price": float(item.get("bid_price", 0)) if item.get("bid_price") else None,
                        "bid_size": int(item.get("bid_size", 0)) if item.get("bid_size") else None,
                        "ask_price": float(item.get("ask_price", 0)) if item.get("ask_price") else None,
                        "ask_size": int(item.get("ask_size", 0)) if item.get("ask_size") else None,
                        "ticker": item.get("ticker"),
                        "session_end_date": item.get("session_end_date"),
                    })
                except Exception as e:
                    logger.warning(f"Error normalizing quote item: {e}")
                    continue
        
        else:
            # For other endpoints (contracts, products, schedules, etc.), return raw results
            data = results if isinstance(results, list) else [results] if results else []
        
        return {
            "provider": self.name,
            "endpoint_type": endpoint_type,
            "data": data,
            "request": request_kwargs,
            "raw_response": raw,  # Include raw response for reference
        }


__all__ = ["MassiveFuturesAdapter"]
