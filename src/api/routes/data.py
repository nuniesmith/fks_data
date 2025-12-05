"""
Standardized REST API routes for market data.
Phase 2.1: Data Flow Stabilization
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel

from ...adapters import get_adapter
from ...adapters.multi_provider_manager import MultiProviderManager
from ...framework.cache import get_cache_backend

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/data", tags=["data"])


class PriceResponse(BaseModel):
    """Price response model"""
    symbol: str
    price: float
    timestamp: int
    provider: str
    cached: bool = False


class OHLCVResponse(BaseModel):
    """OHLCV response model"""
    symbol: str
    interval: str
    data: List[Dict[str, Any]]
    provider: str
    cached: bool = False


@router.get("/price", response_model=PriceResponse)
async def get_price(
    symbol: str = Query(..., description="Asset symbol (e.g., BTCUSDT, AAPL)"),
    provider: Optional[str] = Query(None, description="Specific provider (binance, polygon, etc.)"),
    use_cache: bool = Query(True, description="Use cached data if available")
):
    """
    Get current price for a symbol.
    
    Supports multiple providers with automatic failover.
    """
    try:
        # Check cache first
        cache = get_cache_backend()
        cache_key = f"price:{symbol}:{provider or 'any'}"
        
        if use_cache and cache:
            try:
                # Cache is async
                if hasattr(cache, 'get'):
                    cached_data = await cache.get(cache_key)
                    if cached_data:
                        return PriceResponse(**cached_data, cached=True)
            except Exception as e:
                logger.debug(f"Cache get error: {e}")
        
        # Use MultiProviderManager for failover
        manager = MultiProviderManager()
        result = manager.get_data(
            asset=symbol,
            granularity="1m",
            providers=[provider] if provider else None
        )
        
        if not result or not result.get("data"):
            raise HTTPException(status_code=404, detail=f"No price data found for {symbol}")
        
        # Get latest price
        latest = result["data"][-1] if result["data"] else None
        if not latest:
            raise HTTPException(status_code=404, detail=f"No price data found for {symbol}")
        
        price_data = PriceResponse(
            symbol=symbol,
            price=latest.get("close", 0),
            timestamp=latest.get("ts", 0),
            provider=result.get("provider", "unknown"),
            cached=False
        )
        
        # Cache for 60 seconds
        if cache and use_cache:
            try:
                if hasattr(cache, 'set'):
                    await cache.set(cache_key, price_data.dict(), ttl=60)
            except Exception as e:
                logger.warning(f"Cache set error: {e}")
        
        return price_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching price: {str(e)}")


@router.get("/ohlcv", response_model=OHLCVResponse)
async def get_ohlcv(
    symbol: str = Query(..., description="Asset symbol"),
    interval: str = Query("1h", description="Time interval (1m, 5m, 1h, 1d)"),
    limit: Optional[int] = Query(None, description="Maximum number of candles to fetch"),
    start: Optional[int] = Query(None, description="Start timestamp (Unix)"),
    end: Optional[int] = Query(None, description="End timestamp (Unix)"),
    provider: Optional[str] = Query(None, description="Specific provider"),
    use_cache: bool = Query(True, description="Use cached data")
):
    """
    Get OHLCV (Open, High, Low, Close, Volume) data.
    
    Supports multiple providers with automatic failover.
    """
    try:
        # Check cache
        cache = get_cache_backend()
        cache_key = f"ohlcv:{symbol}:{interval}:{limit}:{start}:{end}"
        
        if use_cache and cache:
            try:
                # Cache is async
                if hasattr(cache, 'get'):
                    cached_data = await cache.get(cache_key)
                    if cached_data:
                        return OHLCVResponse(**cached_data, cached=True)
            except Exception as e:
                logger.debug(f"Cache get error: {e}")
        
        # Fetch data
        manager = MultiProviderManager()
        result = manager.get_data(
            asset=symbol,
            granularity=interval,
            start_date=start,
            end_date=end,
            providers=[provider] if provider else None,
            limit=limit
        )
        
        if not result or not result.get("data"):
            raise HTTPException(status_code=404, detail=f"No OHLCV data found for {symbol}")
        
        ohlcv_data = OHLCVResponse(
            symbol=symbol,
            interval=interval,
            data=result["data"],
            provider=result.get("provider", "unknown"),
            cached=False
        )
        
        # Cache for 5 minutes
        if cache and use_cache:
            try:
                if hasattr(cache, 'set'):
                    await cache.set(cache_key, ohlcv_data.dict(), ttl=300)
            except Exception as e:
                logger.warning(f"Cache set error: {e}")
        
        return ohlcv_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching OHLCV: {str(e)}")


@router.get("/providers")
async def list_providers():
    """List available data providers"""
    return {
        "providers": [
            {"name": "binance", "type": "crypto", "rate_limit": "10 req/sec"},
            {"name": "polygon", "type": "stocks/crypto", "rate_limit": "4 req/sec"},
            {"name": "coingecko", "type": "crypto", "rate_limit": "varies"},
            {"name": "alpha_vantage", "type": "stocks", "rate_limit": "5 req/min"},
            {"name": "cmc", "type": "crypto", "rate_limit": "varies"},
            {"name": "eodhd", "type": "stocks/fundamentals", "rate_limit": "1 req/sec"},
        ]
    }


@router.get("/health")
async def data_health():
    """Data service health check with provider status"""
    providers_status = {}
    
    # Check each provider
    for provider_name in ["binance", "polygon", "coingecko"]:
        try:
            adapter = get_adapter(provider_name)
            # Quick test (could be improved)
            providers_status[provider_name] = "available"
        except Exception as e:
            providers_status[provider_name] = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "service": "fks_data",
        "providers": providers_status,
        "timestamp": datetime.utcnow().isoformat()
    }

