"""
FKS Data Service - Standalone Flask Application

This is a simplified version that doesn't depend on the framework template.
"""

import logging
import os
import sys
from datetime import UTC, datetime, timezone

from flask import Flask, jsonify, request
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# Add src to path for local imports
# Get the directory containing this file (src/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (repo/data/src -> repo/data)
src_dir = os.path.dirname(current_dir) if os.path.basename(current_dir) == 'src' else current_dir
# Add src to path
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Service information
SERVICE_INFO = {
    "name": "fks_data",
    "version": "1.0.0",
    "description": "FKS Data Service - Market data collection and storage",
    "status": "healthy",
    "started_at": datetime.utcnow().isoformat()
}

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "fks_data",
        "timestamp": datetime.now(UTC).isoformat(),
        "version": "1.0.0"
    })

@app.route('/info', methods=['GET'])
def info():
    """Service information endpoint"""
    return jsonify(SERVICE_INFO)

@app.route('/status', methods=['GET'])
def status():
    """Detailed status endpoint"""
    return jsonify({
        **SERVICE_INFO,
        "uptime": str(datetime.utcnow() - datetime.fromisoformat(SERVICE_INFO["started_at"].replace('Z', '+00:00'))),
        "endpoints": {
            "health": "/health",
            "info": "/info",
            "status": "/status",
            "data": "/api/v1/data"
        }
    })

@app.route('/api/v1/data', methods=['GET'])
def get_data():
    """Sample data endpoint"""
    return jsonify({
        "message": "FKS Data Service is operational",
        "data": {
            "timestamp": datetime.utcnow().isoformat(),
            "sample_data": "Market data would be here",
            "status": "ready"
        }
    })

# Import and register data routes - inline implementation to avoid import issues
try:
    from adapters.multi_provider_manager import MultiProviderManager
    
    # Try to import cache, but make it optional
    try:
        from framework.cache import get_cache_backend
        cache_available = True
    except ImportError:
        logger.warning("⚠️ Cache backend not available - caching disabled")
        get_cache_backend = lambda: None
        cache_available = False
    
    @app.route('/api/v1/data/price', methods=['GET'])
    def price_endpoint():
        """Get current price for a symbol"""
        try:
            symbol = request.args.get('symbol')
            if not symbol:
                return jsonify({"error": "symbol parameter required"}), 400
            
            provider = request.args.get('provider')
            use_cache = request.args.get('use_cache', 'true').lower() == 'true'
            
            # Check cache first (if available)
            cache = None
            if cache_available:
                try:
                    cache = get_cache_backend()
                except Exception as e:
                    logger.debug(f"Cache backend not available: {e}")
                    cache = None
            
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
                    logger.debug(f"Cache get error: {e}")
            
            # Use MultiProviderManager for failover
            manager = MultiProviderManager()
            result = manager.get_data(
                asset=symbol,
                granularity="1m",
                providers=[provider] if provider else None,
                limit=1
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
    
    @app.route('/api/v1/data/ohlcv', methods=['GET'])
    def ohlcv_endpoint():
        """Get OHLCV (Open, High, Low, Close, Volume) data"""
        try:
            symbol = request.args.get('symbol')
            if not symbol:
                return jsonify({"error": "symbol parameter required"}), 400
            
            interval = request.args.get('interval', '1h')
            limit = request.args.get('limit', type=int)
            start = request.args.get('start', type=int)
            end = request.args.get('end', type=int)
            provider = request.args.get('provider')
            use_cache = request.args.get('use_cache', 'true').lower() == 'true'
            
            # Check cache (if available)
            cache = None
            if cache_available:
                try:
                    cache = get_cache_backend()
                except Exception as e:
                    logger.debug(f"Cache backend not available: {e}")
                    cache = None
            
            cache_key = f"ohlcv:{symbol}:{interval}:{limit}:{start}:{end}"
            
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
    
    @app.route('/api/v1/data/providers', methods=['GET'])
    def providers_endpoint():
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
    
    logger.info("✅ Registered data API routes: /api/v1/data/price, /api/v1/data/ohlcv")
except ImportError as e:
    logger.warning(f"⚠️ Could not import data API dependencies: {e}")
    import traceback
    logger.warning(f"⚠️ Import error details: {traceback.format_exc()}")
    logger.warning("⚠️ Price and OHLCV endpoints will not be available")

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "fks_data",
        "message": "FKS Data Service is running",
        "health_check": "/health",
        "api_docs": "/info"
    })

if __name__ == '__main__':
    logger.info("🚀 Starting FKS Data Service")
    logger.info(f"📊 Service: {SERVICE_INFO['name']} v{SERVICE_INFO['version']}")

    # Get configuration from environment
    host = os.getenv('FKS_DATA_HOST', '0.0.0.0')
    port = int(os.getenv('FKS_DATA_PORT', '4200'))
    debug = os.getenv('FKS_DEBUG', 'false').lower() == 'true'

    logger.info(f"🌐 Starting server on {host}:{port}")

    try:
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True
        )
    except Exception as e:
        logger.error(f"❌ Failed to start server: {e}")
        sys.exit(1)
