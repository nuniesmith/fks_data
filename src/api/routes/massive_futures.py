"""FastAPI routes for Massive.com Futures API.

Provides REST endpoints for:
- Contracts (list, details)
- Products (list, details)
- Schedules (all, product-specific)
- Aggregate bars (OHLC)
- Trades
- Quotes
- Market status
- Exchanges
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Query, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from ...adapters import get_adapter
from ...adapters.massive_futures_ws import MassiveFuturesWebSocket

# Import logger
import logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/futures", tags=["futures"])


# ==================== Request/Response Models ====================

class FuturesAggsRequest(BaseModel):
    """Request model for aggregate bars."""
    ticker: str
    resolution: str = "1min"  # e.g., "1min", "5min", "1hour", "1day"
    window_start: Optional[str] = None  # YYYY-MM-DD or nanosecond timestamp
    limit: Optional[int] = 1000
    sort: Optional[str] = "window_start.desc"


class FuturesTradesRequest(BaseModel):
    """Request model for trades."""
    ticker: str
    timestamp: Optional[str] = None  # YYYY-MM-DD or nanosecond timestamp
    session_end_date: Optional[str] = None  # YYYY-MM-DD
    limit: Optional[int] = 1000
    sort: Optional[str] = "timestamp.desc"


class FuturesQuotesRequest(BaseModel):
    """Request model for quotes."""
    ticker: str
    timestamp: Optional[str] = None
    session_end_date: Optional[str] = None
    limit: Optional[int] = 1000
    sort: Optional[str] = "timestamp.desc"


# ==================== Contracts Endpoints ====================

@router.get("/contracts")
async def get_contracts(
    product_code: Optional[str] = Query(None, description="Filter by product code"),
    first_trade_date: Optional[str] = Query(None, description="First trade date (YYYY-MM-DD)"),
    last_trade_date: Optional[str] = Query(None, description="Last trade date (YYYY-MM-DD)"),
    as_of: Optional[str] = Query(None, description="Point-in-time date (YYYY-MM-DD)"),
    active: Optional[str] = Query(None, description="Filter by active status (true/false/all)"),
    type: Optional[str] = Query("all", description="Contract type (all/single/combo)"),
    limit: int = Query(100, ge=1, le=1000, description="Results per page"),
    sort: Optional[str] = Query(None, description="Sort field and direction (e.g., ticker.asc)"),
):
    """Get list of futures contracts with filtering options."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "contracts",
            "limit": limit,
        }
        if product_code:
            kwargs["product_code"] = product_code
        if first_trade_date:
            kwargs["first_trade_date"] = first_trade_date
        if last_trade_date:
            kwargs["last_trade_date"] = last_trade_date
        if as_of:
            kwargs["as_of"] = as_of
        if active:
            kwargs["active"] = active
        if type:
            kwargs["type"] = type
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching contracts: {str(e)}")


@router.get("/contracts/{ticker}")
async def get_contract(
    ticker: str,
    as_of: Optional[str] = Query(None, description="Point-in-time date (YYYY-MM-DD)"),
):
    """Get detailed information about a specific futures contract."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "contract",
            "ticker": ticker,
        }
        if as_of:
            kwargs["as_of"] = as_of
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching contract: {str(e)}")


# ==================== Products Endpoints ====================

@router.get("/products")
async def get_products(
    name: Optional[str] = Query(None, description="Product name (exact match)"),
    as_of: Optional[str] = Query(None, description="Point-in-time date (YYYY-MM-DD)"),
    trading_venue: Optional[str] = Query(None, description="Trading venue (MIC)"),
    sector: Optional[str] = Query(None, description="Sector filter"),
    sub_sector: Optional[str] = Query(None, description="Sub-sector filter"),
    asset_class: Optional[str] = Query(None, description="Asset class filter"),
    asset_sub_class: Optional[str] = Query(None, description="Asset sub-class filter"),
    type: Optional[str] = Query("all", description="Product type (all/single/combo)"),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None, description="Sort field and direction"),
):
    """Get list of futures products with filtering options."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "products",
            "limit": limit,
        }
        if name:
            kwargs["name"] = name
        if as_of:
            kwargs["as_of"] = as_of
        if trading_venue:
            kwargs["trading_venue"] = trading_venue
        if sector:
            kwargs["sector"] = sector
        if sub_sector:
            kwargs["sub_sector"] = sub_sector
        if asset_class:
            kwargs["asset_class"] = asset_class
        if asset_sub_class:
            kwargs["asset_sub_class"] = asset_sub_class
        if type:
            kwargs["type"] = type
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching products: {str(e)}")


@router.get("/products/{product_code}")
async def get_product(
    product_code: str,
    type: Optional[str] = Query("single", description="Product type (single/combo)"),
    as_of: Optional[str] = Query(None, description="Point-in-time date (YYYY-MM-DD)"),
):
    """Get detailed information about a specific futures product."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "product",
            "product_code": product_code,
        }
        if type:
            kwargs["type"] = type
        if as_of:
            kwargs["as_of"] = as_of
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product: {str(e)}")


# ==================== Schedules Endpoints ====================

@router.get("/schedules")
async def get_schedules(
    session_end_date: Optional[str] = Query(None, description="Trading date (YYYY-MM-DD)"),
    trading_venue: Optional[str] = Query(None, description="Trading venue (MIC)"),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None, description="Sort field and direction"),
):
    """Get trading schedules for futures contracts."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "schedules",
            "limit": limit,
        }
        if session_end_date:
            kwargs["session_end_date"] = session_end_date
        if trading_venue:
            kwargs["trading_venue"] = trading_venue
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching schedules: {str(e)}")


@router.get("/products/{product_code}/schedules")
async def get_product_schedules(
    product_code: str,
    session_end_date: Optional[str] = Query(None, description="Trading date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None, description="Sort field and direction"),
):
    """Get trading schedules for a specific futures product."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "product_schedules",
            "product_code": product_code,
            "limit": limit,
        }
        if session_end_date:
            kwargs["session_end_date"] = session_end_date
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product schedules: {str(e)}")


# ==================== Aggregate Bars (OHLC) Endpoint ====================

@router.get("/aggs/{ticker}")
async def get_aggregates(
    ticker: str,
    resolution: str = Query("1min", description="Bar resolution (e.g., 1min, 5min, 1hour, 1day)"),
    window_start: Optional[str] = Query(None, description="Start time (YYYY-MM-DD or nanosecond timestamp)"),
    window_start_gte: Optional[str] = Query(None, description="Start time >= (YYYY-MM-DD or timestamp)"),
    window_start_gt: Optional[str] = Query(None, description="Start time > (YYYY-MM-DD or timestamp)"),
    window_start_lte: Optional[str] = Query(None, description="Start time <= (YYYY-MM-DD or timestamp)"),
    window_start_lt: Optional[str] = Query(None, description="Start time < (YYYY-MM-DD or timestamp)"),
    limit: int = Query(1000, ge=1, le=50000),
    sort: Optional[str] = Query("window_start.desc", description="Sort field and direction"),
):
    """Get aggregated OHLC bars for a futures contract."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "aggs",
            "ticker": ticker,
            "resolution": resolution,
            "limit": limit,
        }
        if window_start:
            kwargs["window_start"] = window_start
        if window_start_gte:
            kwargs["window_start.gte"] = window_start_gte
        if window_start_gt:
            kwargs["window_start.gt"] = window_start_gt
        if window_start_lte:
            kwargs["window_start.lte"] = window_start_lte
        if window_start_lt:
            kwargs["window_start.lt"] = window_start_lt
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching aggregates: {str(e)}")


# ==================== Trades Endpoint ====================

@router.get("/trades/{ticker}")
async def get_trades(
    ticker: str,
    timestamp: Optional[str] = Query(None, description="Trade timestamp (YYYY-MM-DD or nanosecond timestamp)"),
    session_end_date: Optional[str] = Query(None, description="Trading date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=50000),
    sort: Optional[str] = Query("timestamp.desc", description="Sort field and direction"),
):
    """Get tick-level trade data for a futures contract."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "trades",
            "ticker": ticker,
            "limit": limit,
        }
        if timestamp:
            kwargs["timestamp"] = timestamp
        if session_end_date:
            kwargs["session_end_date"] = session_end_date
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trades: {str(e)}")


# ==================== Quotes Endpoint ====================

@router.get("/quotes/{ticker}")
async def get_quotes(
    ticker: str,
    timestamp: Optional[str] = Query(None, description="Quote timestamp (YYYY-MM-DD or nanosecond timestamp)"),
    session_end_date: Optional[str] = Query(None, description="Trading date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=50000),
    sort: Optional[str] = Query("timestamp.desc", description="Sort field and direction"),
):
    """Get quote data (bid/ask) for a futures contract."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "quotes",
            "ticker": ticker,
            "limit": limit,
        }
        if timestamp:
            kwargs["timestamp"] = timestamp
        if session_end_date:
            kwargs["session_end_date"] = session_end_date
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching quotes: {str(e)}")


# ==================== Market Status Endpoint ====================

@router.get("/market-status")
async def get_market_status(
    product_code: Optional[str] = Query(None, description="Filter by product code"),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None, description="Sort field and direction"),
):
    """Get current market status for futures products."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "market_status",
            "limit": limit,
        }
        if product_code:
            kwargs["product_code"] = product_code
        if sort:
            kwargs["sort"] = sort
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching market status: {str(e)}")


# ==================== Exchanges Endpoint ====================

@router.get("/exchanges")
async def get_exchanges(
    limit: int = Query(100, ge=1, le=999, description="Maximum number of results"),
):
    """Get list of supported futures exchanges."""
    try:
        adapter = get_adapter("massive_futures")
        kwargs = {
            "endpoint_type": "exchanges",
            "limit": limit,
        }
        
        result = adapter.fetch(**kwargs)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching exchanges: {str(e)}")


# ==================== WebSocket Endpoint ====================

@router.websocket("/ws")
async def futures_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time futures data.
    
    Supports subscriptions to:
    - Trades: Subscribe to real-time trades
    - Quotes: Subscribe to real-time quotes
    - Aggregates: Subscribe to real-time OHLC bars
    
    Message format:
    {
        "action": "subscribe",
        "type": "trades|quotes|aggregates",
        "tickers": ["ESU0", "GCJ5"],
        "resolution": "1min"  # Only for aggregates
    }
    """
    await websocket.accept()
    
    ws_client: Optional[MassiveFuturesWebSocket] = None
    
    try:
        # Initialize WebSocket client
        ws_client = MassiveFuturesWebSocket(
            on_message=lambda msg: None,  # We'll handle messages manually
        )
        await ws_client.connect()
        
        # Send connection confirmation
        await websocket.send_json({
            "type": "status",
            "status": "connected",
            "message": "Connected to Massive Futures WebSocket"
        })
        
        # Listen for client messages and WebSocket data
        async def forward_messages():
            """Forward messages from Massive WebSocket to client."""
            try:
                async for message in ws_client.listen():
                    await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error forwarding WebSocket messages: {e}")
        
        # Start forwarding messages
        import asyncio
        forward_task = asyncio.create_task(forward_messages())
        
        # Handle client messages
        while True:
            try:
                data = await websocket.receive_json()
                action = data.get("action")
                
                if action == "subscribe":
                    msg_type = data.get("type")
                    tickers = data.get("tickers", [])
                    
                    if msg_type == "trades":
                        await ws_client.subscribe_trades(tickers)
                        await websocket.send_json({
                            "type": "status",
                            "status": "subscribed",
                            "message": f"Subscribed to trades for {tickers}"
                        })
                    elif msg_type == "quotes":
                        await ws_client.subscribe_quotes(tickers)
                        await websocket.send_json({
                            "type": "status",
                            "status": "subscribed",
                            "message": f"Subscribed to quotes for {tickers}"
                        })
                    elif msg_type == "aggregates":
                        resolution = data.get("resolution", "1min")
                        await ws_client.subscribe_aggregates(tickers, resolution)
                        await websocket.send_json({
                            "type": "status",
                            "status": "subscribed",
                            "message": f"Subscribed to aggregates ({resolution}) for {tickers}"
                        })
                
                elif action == "unsubscribe":
                    msg_type = data.get("type", "all")
                    tickers = data.get("tickers", [])
                    await ws_client.unsubscribe(tickers, msg_type)
                    await websocket.send_json({
                        "type": "status",
                        "status": "unsubscribed",
                        "message": f"Unsubscribed from {msg_type} for {tickers}"
                    })
                
                else:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Unknown action: {action}"
                    })
            
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}")
                await websocket.send_json({
                    "type": "error",
                    "message": str(e)
                })
        
        # Cancel forwarding task
        forward_task.cancel()
        try:
            await forward_task
        except asyncio.CancelledError:
            pass
    
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(e)
            })
        except:
            pass
    finally:
        if ws_client:
            await ws_client.disconnect()
        try:
            await websocket.close()
        except:
            pass
