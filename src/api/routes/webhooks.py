"""
Webhook endpoints for real-time data updates.
Phase 2.1: Data Flow Stabilization
"""

import hashlib
import hmac
import json
import logging
from typing import Dict, Any, Optional
from fastapi import APIRouter, Request, HTTPException, Header, Depends
from pydantic import BaseModel

from ...framework.cache import get_cache_backend

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
logger = logging.getLogger(__name__)


class WebhookPayload(BaseModel):
    """Generic webhook payload"""
    data: Dict[str, Any]
    timestamp: Optional[int] = None


def verify_binance_signature(
    payload: bytes,
    signature: str,
    secret: str
) -> bool:
    """Verify Binance webhook signature"""
    expected_signature = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected_signature, signature)


@router.post("/binance")
async def binance_webhook(
    request: Request,
    x_binance_signature: Optional[str] = Header(None, alias="X-Binance-Signature")
):
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
        payload = await request.body()
        data = json.loads(payload)
        
        # Verify signature if provided
        secret = request.app.state.get("BINANCE_WEBHOOK_SECRET")
        if secret and x_binance_signature:
            if not verify_binance_signature(payload, x_binance_signature, secret):
                raise HTTPException(status_code=401, detail="Invalid signature")
        
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
                cache_key = f"webhook:{symbol}:{normalized['ts']}"
                await cache.set(cache_key, normalized, ttl=3600)
            
            # Publish to Redis for other services
            # TODO: Integrate with Redis pub/sub
            
            logger.info(f"Received Binance webhook for {symbol}")
            return {"status": "ok", "symbol": symbol}
        
        return {"status": "ok", "message": "Event processed"}
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing Binance webhook: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/polygon")
async def polygon_webhook(
    request: Request,
    x_polygon_signature: Optional[str] = Header(None, alias="X-Polygon-Signature")
):
    """
    Receive webhook from Polygon for real-time market data updates.
    """
    try:
        payload = await request.body()
        data = json.loads(payload)
        
        # Verify signature if provided
        secret = request.app.state.get("POLYGON_WEBHOOK_SECRET")
        if secret and x_polygon_signature:
            # Polygon uses HMAC-SHA256
            expected = hmac.new(
                secret.encode(),
                payload,
                hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(expected, x_polygon_signature):
                raise HTTPException(status_code=401, detail="Invalid signature")
        
        # Process Polygon data
        # Format depends on Polygon webhook type
        logger.info(f"Received Polygon webhook: {data.get('event', 'unknown')}")
        
        # Store in cache
        cache = get_cache_backend()
        if cache and "data" in data:
            cache_key = f"webhook:polygon:{data.get('symbol', 'unknown')}"
            await cache.set(cache_key, data, ttl=3600)
        
        return {"status": "ok"}
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing Polygon webhook: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/test")
async def test_webhook():
    """Test endpoint to verify webhook system is working"""
    return {
        "status": "ok",
        "message": "Webhook system is operational",
        "endpoints": {
            "binance": "/webhooks/binance",
            "polygon": "/webhooks/polygon"
        }
    }

