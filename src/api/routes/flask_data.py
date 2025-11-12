"""
Flask-compatible routes for market data API.
Phase 2.1: Data Flow Stabilization
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from flask import request, jsonify

from ...adapters import get_adapter
from ...adapters.multi_provider_manager import MultiProviderManager
from ...framework.cache import get_cache_backend

logger = logging.getLogger(__name__)


def get_price():
    """
    Get current price for a symbol.
    
    Query params:
        symbol: Asset symbol (e.g., BTCUSDT, AAPL)
        provider: Optional specific provider (binance, polygon, etc.)
        use_cache: Use cached data if available (default: true)
    """
    try:
        symbol = request.args.get('symbol')
        if not symbol:
            return jsonify({"error": "symbol parameter required"}), 400
        
        provider = request.args.get('provider')
        use_cache = request.args.get('use_cache', 'true').lower() == 'true'
        
        # Check cache first
        cache = get_cache_backend()
        cache_key = f"price:{symbol}:{provider or 'any'}"
        
        if use_cache and cache:
            try:
                cached_data = cache.get(cache_key)
                if cached_data:
                    return jsonify({
                        "symbol": symbol,
                        "price": cached_data.get("price"),
                        "timestamp": cached_data.get("timestamp"),
                        "provider": cached_data.get("provider"),
                        "cached": True
                    })
            except Exception as e:
                logger.warning(f"Cache get error: {e}")
        
        # Use MultiProviderManager for failover
        manager = MultiProviderManager()
        result = manager.get_data(
            asset=symbol,
            granularity="1m",
            providers=[provider] if provider else None
        )
        
        if not result or not result.get("data"):
            return jsonify({"error": f"No price data found for {symbol}"}), 404
        
        # Get latest price
        latest = result["data"][-1] if result["data"] else None
        if not latest:
            return jsonify({"error": f"No price data found for {symbol}"}), 404
        
        price_data = {
            "symbol": symbol,
            "price": latest.get("close", 0),
            "timestamp": latest.get("ts", 0),
            "provider": result.get("provider", "unknown"),
            "cached": False
        }
        
        # Cache for 60 seconds
        if cache and use_cache:
            try:
                cache.set(cache_key, price_data, ttl=60)
            except Exception as e:
                logger.warning(f"Cache set error: {e}")
        
        return jsonify(price_data)
        
    except Exception as e:
        logger.error(f"Error fetching price: {e}")
        return jsonify({"error": f"Error fetching price: {str(e)}"}), 500


def get_ohlcv():
    """
    Get OHLCV (Open, High, Low, Close, Volume) data.
    
    Query params:
        symbol: Asset symbol
        interval: Time interval (1m, 5m, 1h, 1d) - default: 1h
        start: Optional start timestamp (Unix)
        end: Optional end timestamp (Unix)
        provider: Optional specific provider
        use_cache: Use cached data (default: true)
    """
    try:
        symbol = request.args.get('symbol')
        if not symbol:
            return jsonify({"error": "symbol parameter required"}), 400
        
        interval = request.args.get('interval', '1h')
        start = request.args.get('start', type=int)
        end = request.args.get('end', type=int)
        provider = request.args.get('provider')
        use_cache = request.args.get('use_cache', 'true').lower() == 'true'
        
        # Check cache
        cache = get_cache_backend()
        cache_key = f"ohlcv:{symbol}:{interval}:{start}:{end}"
        
        if use_cache and cache:
            try:
                cached_data = cache.get(cache_key)
                if cached_data:
                    return jsonify({
                        "symbol": symbol,
                        "interval": interval,
                        "data": cached_data.get("data", []),
                        "provider": cached_data.get("provider"),
                        "cached": True
                    })
            except Exception as e:
                logger.warning(f"Cache get error: {e}")
        
        # Fetch data
        manager = MultiProviderManager()
        result = manager.get_data(
            asset=symbol,
            granularity=interval,
            start_date=start,
            end_date=end,
            providers=[provider] if provider else None
        )
        
        if not result or not result.get("data"):
            return jsonify({"error": f"No OHLCV data found for {symbol}"}), 404
        
        ohlcv_data = {
            "symbol": symbol,
            "interval": interval,
            "data": result["data"],
            "provider": result.get("provider", "unknown"),
            "cached": False
        }
        
        # Cache for 5 minutes
        if cache and use_cache:
            try:
                cache.set(cache_key, ohlcv_data, ttl=300)
            except Exception as e:
                logger.warning(f"Cache set error: {e}")
        
        return jsonify(ohlcv_data)
        
    except Exception as e:
        logger.error(f"Error fetching OHLCV: {e}")
        return jsonify({"error": f"Error fetching OHLCV: {str(e)}"}), 500


def list_providers():
    """List available data providers"""
    return jsonify({
        "providers": [
            {"name": "binance", "type": "crypto", "rate_limit": "10 req/sec"},
            {"name": "polygon", "type": "stocks/crypto", "rate_limit": "4 req/sec"},
            {"name": "coingecko", "type": "crypto", "rate_limit": "varies"},
            {"name": "alpha_vantage", "type": "stocks", "rate_limit": "5 req/min"},
            {"name": "cmc", "type": "crypto", "rate_limit": "varies"},
            {"name": "eodhd", "type": "stocks/fundamentals", "rate_limit": "1 req/sec"},
        ]
    })


def data_health():
    """Data service health check with provider status"""
    providers_status = {}
    
    # Check each provider
    for provider_name in ["binance", "polygon", "coingecko"]:
        try:
            adapter = get_adapter(provider_name)
            providers_status[provider_name] = "available"
        except Exception as e:
            providers_status[provider_name] = f"error: {str(e)}"
    
    return jsonify({
        "status": "healthy",
        "service": "fks_data",
        "providers": providers_status,
        "timestamp": datetime.utcnow().isoformat()
    })

