"""
Flask-compatible webhook endpoints for real-time data updates.
Phase 2.1: Data Flow Stabilization
"""

import hashlib
import hmac
import json
import logging
from flask import request, jsonify

from ...framework.cache import get_cache_backend

logger = logging.getLogger(__name__)


def verify_binance_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Verify Binance webhook signature"""
    expected_signature = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected_signature, signature)


def binance_webhook():
    """
    Receive webhook from Binance for real-time market data updates.
    
    Expected payload format:
    {
        "e": "kline",  // Event type
        "s": "BTCUSDT", // Symbol
        "k": {
            "t": 1234567890,  // Open time
            "o": "50000",     // Open price
            "h": "51000",     // High price
            "l": "49000",     // Low price
            "c": "50500",     // Close price
            "v": "100.5",     // Volume
            "x": true         // Is closed
        }
    }
    """
    try:
        payload = request.get_data()
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        
        # Verify signature if provided
        signature = request.headers.get('X-Binance-Signature')
        secret = request.environ.get('BINANCE_WEBHOOK_SECRET') or request.app.config.get('BINANCE_WEBHOOK_SECRET')
        
        if secret and signature:
            if not verify_binance_signature(payload, signature, secret):
                return jsonify({"error": "Invalid signature"}), 401
        
        # Process kline data
        if data.get("e") == "kline" and data.get("k", {}).get("x"):
            kline = data["k"]
            symbol = data.get("s", "")
            
            # Normalize data
            normalized = {
                "symbol": symbol,
                "ts": kline.get("t", 0) // 1000,  # Convert to seconds
                "open": float(kline.get("o", 0)),
                "high": float(kline.get("h", 0)),
                "low": float(kline.get("l", 0)),
                "close": float(kline.get("c", 0)),
                "volume": float(kline.get("v", 0)),
                "provider": "binance",
                "source": "webhook"
            }
            
            # Store in cache
            cache = get_cache_backend()
            if cache:
                try:
                    cache_key = f"webhook:{symbol}:{normalized['ts']}"
                    cache.set(cache_key, normalized, ttl=3600)
                except Exception as e:
                    logger.warning(f"Cache set error: {e}")
            
            # Publish to Redis for other services
            # TODO: Integrate with Redis pub/sub
            
            logger.info(f"Received Binance webhook for {symbol}")
            return jsonify({"status": "ok", "symbol": symbol})
        
        return jsonify({"status": "ok", "message": "Event processed"})
        
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON payload"}), 400
    except Exception as e:
        logger.error(f"Error processing Binance webhook: {e}")
        return jsonify({"error": f"Error: {str(e)}"}), 500


def polygon_webhook():
    """
    Receive webhook from Polygon for real-time market data updates.
    """
    try:
        payload = request.get_data()
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        
        # Verify signature if provided
        signature = request.headers.get('X-Polygon-Signature')
        secret = request.environ.get('POLYGON_WEBHOOK_SECRET') or request.app.config.get('POLYGON_WEBHOOK_SECRET')
        
        if secret and signature:
            # Polygon uses HMAC-SHA256
            expected = hmac.new(
                secret.encode(),
                payload,
                hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(expected, signature):
                return jsonify({"error": "Invalid signature"}), 401
        
        # Process Polygon data
        logger.info(f"Received Polygon webhook: {data.get('event', 'unknown')}")
        
        # Store in cache
        cache = get_cache_backend()
        if cache and "data" in data:
            try:
                cache_key = f"webhook:polygon:{data.get('symbol', 'unknown')}"
                cache.set(cache_key, data, ttl=3600)
            except Exception as e:
                logger.warning(f"Cache set error: {e}")
        
        return jsonify({"status": "ok"})
        
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON payload"}), 400
    except Exception as e:
        logger.error(f"Error processing Polygon webhook: {e}")
        return jsonify({"error": f"Error: {str(e)}"}), 500


def test_webhook():
    """Test endpoint to verify webhook system is working"""
    return jsonify({
        "status": "ok",
        "message": "Webhook system is operational",
        "endpoints": {
            "binance": "/webhooks/binance",
            "polygon": "/webhooks/polygon"
        }
    })

