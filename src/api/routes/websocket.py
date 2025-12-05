"""
WebSocket endpoint for real-time market data streaming.

Provides WebSocket interface for subscribing to real-time OHLCV data
from multiple providers (Binance, Polygon, etc.).

Endpoint: ws://fks_data:8003/api/v1/data/ws
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import websockets

from ...adapters.multi_provider_manager import MultiProviderManager

logger = logging.getLogger(__name__)

# Import Binance WebSocket client
try:
    from ...adapters.binance_ws import BinanceWebSocketClient
    HAS_BINANCE_WS = True
except ImportError:
    HAS_BINANCE_WS = False
    logger.warning("Binance WebSocket client not available")

router = APIRouter(prefix="/api/v1/data", tags=["data"])


class MarketDataWebSocketManager:
    """Manages WebSocket connections and subscriptions for market data"""
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.subscriptions: Dict[WebSocket, Dict[str, Any]] = {}  # ws -> {symbols, timeframes}
        self.provider_manager = MultiProviderManager()
        self.running = False
        
        # Exchange WebSocket clients
        self.binance_ws: Optional[BinanceWebSocketClient] = None
        self.binance_listen_task: Optional[asyncio.Task] = None
    
    async def connect(self, websocket: WebSocket):
        """Accept WebSocket connection (deprecated - accept in endpoint)"""
        # Note: websocket.accept() should be called in the endpoint handler
        # This method is kept for backward compatibility but may not be used
        if websocket.client_state.name != "CONNECTED":
            await websocket.accept()
        self.active_connections.add(websocket)
        self.subscriptions[websocket] = {"symbols": [], "timeframes": []}
        logger.info(f"WebSocket client connected. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.subscriptions:
            del self.subscriptions[websocket]
        logger.info(f"WebSocket client disconnected. Total: {len(self.active_connections)}")
    
    async def send_message(self, websocket: WebSocket, message: Dict[str, Any]):
        """Send message to WebSocket client"""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Error sending WebSocket message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: Dict[str, Any], symbol: Optional[str] = None):
        """Broadcast message to all subscribed clients"""
        for websocket in list(self.active_connections):
            # Check if client is subscribed to this symbol
            if symbol:
                subs = self.subscriptions.get(websocket, {})
                if symbol not in subs.get("symbols", []):
                    continue
            
            try:
                await self.send_message(websocket, message)
            except Exception:
                pass  # Already handled in send_message
    
    async def start_binance_streaming(self):
        """Start Binance WebSocket streaming if not already running."""
        if not HAS_BINANCE_WS:
            logger.warning("Binance WebSocket client not available")
            return
        
        if self.binance_ws and self.binance_ws.connected:
            logger.debug("Binance WebSocket already running")
            return
        
        try:
            # Initialize Binance WebSocket client
            self.binance_ws = BinanceWebSocketClient(
                on_message=self._handle_binance_message,
                on_error=self._handle_binance_error
            )
            
            await self.binance_ws.connect()
            
            # Start listening task
            self.binance_listen_task = asyncio.create_task(self._binance_listen_loop())
            
            logger.info("Binance WebSocket streaming started")
        except Exception as e:
            logger.error(f"Failed to start Binance WebSocket streaming: {e}")
    
    def _handle_binance_message(self, message: Dict[str, Any]):
        """Handle message from Binance WebSocket (sync callback)."""
        # Forward to subscribed clients (async operation)
        symbol = message.get("symbol")
        timeframe = message.get("timeframe")
        
        # Format message for clients
        client_message = {
            "type": "ohlcv",
            "symbol": symbol,
            "timeframe": timeframe,
            "data": {
                "ts": message.get("ts"),
                "open": message.get("open"),
                "high": message.get("high"),
                "low": message.get("low"),
                "close": message.get("close"),
                "volume": message.get("volume"),
                "is_closed": message.get("is_closed")
            },
            "timestamp": message.get("timestamp", datetime.utcnow().isoformat())
        }
        
        # Schedule broadcast (async operation in sync callback)
        asyncio.create_task(self.broadcast(client_message, symbol=symbol))
    
    def _handle_binance_error(self, error: Exception):
        """Handle error from Binance WebSocket."""
        logger.error(f"Binance WebSocket error: {error}")
        # Could implement reconnection logic here
    
    async def _binance_listen_loop(self):
        """Background task to listen for Binance WebSocket messages."""
        try:
            async for message in self.binance_ws.listen():
                # Messages are handled via on_message callback
                pass
        except Exception as e:
            logger.error(f"Binance listen loop error: {e}")
            self.binance_ws = None
            self.binance_listen_task = None
    
    async def update_binance_subscriptions(self):
        """Update Binance subscriptions based on all client subscriptions."""
        if not HAS_BINANCE_WS or not self.binance_ws:
            return
        
        # Collect all unique symbols and timeframes from all clients
        all_symbols: Set[str] = set()
        all_timeframes: Set[str] = set()
        
        for subs in self.subscriptions.values():
            all_symbols.update(subs.get("symbols", []))
            all_timeframes.update(subs.get("timeframes", []))
        
        if all_symbols and all_timeframes:
            try:
                await self.binance_ws.subscribe(
                    list(all_symbols),
                    list(all_timeframes)
                )
            except Exception as e:
                logger.error(f"Failed to update Binance subscriptions: {e}")


# Global WebSocket manager
ws_manager = MarketDataWebSocketManager()


@router.websocket("/ws")
async def market_data_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for real-time market data.
    
    Supports subscriptions to multiple symbols and timeframes.
    
    Message format for subscription:
    {
        "action": "subscribe",
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "timeframes": ["1m", "5m", "1h"],
        "provider": "binance"  # optional
    }
    
    Message format for unsubscription:
    {
        "action": "unsubscribe",
        "symbols": ["BTCUSDT"]  # optional, empty = unsubscribe all
    }
    
    Response messages:
    {
        "type": "ohlcv" | "status" | "error",
        "symbol": "BTCUSDT",
        "timeframe": "1h",
        "data": {...},  # OHLCV data
        "timestamp": "2025-01-15T12:00:00Z"
    }
    """
    # Accept WebSocket connection first (before any other operations)
    try:
        await websocket.accept()
        logger.info(f"WebSocket connection accepted from {websocket.client}")
    except Exception as e:
        logger.error(f"Failed to accept WebSocket connection: {e}")
        return
    
    # Add to manager after successful accept
    ws_manager.active_connections.add(websocket)
    ws_manager.subscriptions[websocket] = {"symbols": [], "timeframes": []}
    
    try:
        # Send connection confirmation
        try:
            await websocket.send_json({
                "type": "status",
                "status": "connected",
                "message": "Connected to fks_data market data WebSocket",
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            logger.error(f"Error sending connection confirmation: {e}")
            return
        
        # Handle client messages
        while True:
            try:
                # Receive message from client
                data = await websocket.receive_json()
                action = data.get("action")
                
                if action == "subscribe":
                    symbols = data.get("symbols", [])
                    timeframes = data.get("timeframes", ["1m", "5m", "15m", "1h", "4h", "1d"])
                    provider = data.get("provider")
                    
                    # Update subscriptions
                    ws_manager.subscriptions[websocket] = {
                        "symbols": symbols,
                        "timeframes": timeframes,
                        "provider": provider
                    }
                    
                    try:
                        await websocket.send_json({
                            "type": "status",
                            "status": "subscribed",
                            "message": f"Subscribed to {len(symbols)} symbols, {len(timeframes)} timeframes",
                            "symbols": symbols,
                            "timeframes": timeframes,
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    except Exception as e:
                        logger.error(f"Error sending subscription confirmation: {e}")
                    
                    logger.info(f"Client subscribed to {symbols} on {timeframes}")
                    
                    # Start Binance streaming if provider is binance (or default)
                    provider = provider or "binance"
                    if provider == "binance" and HAS_BINANCE_WS:
                        await ws_manager.start_binance_streaming()
                        await ws_manager.update_binance_subscriptions()
                    
                elif action == "unsubscribe":
                    symbols = data.get("symbols", [])
                    
                    if not symbols:
                        # Unsubscribe from all
                        ws_manager.subscriptions[websocket] = {
                            "symbols": [],
                            "timeframes": [],
                            "provider": None
                        }
                    else:
                        # Unsubscribe from specific symbols
                        subs = ws_manager.subscriptions.get(websocket, {})
                        current_symbols = subs.get("symbols", [])
                        ws_manager.subscriptions[websocket]["symbols"] = [
                            s for s in current_symbols if s not in symbols
                        ]
                    
                    try:
                        await websocket.send_json({
                            "type": "status",
                            "status": "unsubscribed",
                            "message": f"Unsubscribed from {symbols if symbols else 'all'}",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    except Exception as e:
                        logger.error(f"Error sending unsubscription confirmation: {e}")
                    
                    # Update Binance subscriptions
                    if HAS_BINANCE_WS:
                        await ws_manager.update_binance_subscriptions()
                    
                elif action == "ping":
                    # Heartbeat
                    try:
                        await websocket.send_json({
                            "type": "pong",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    except Exception as e:
                        logger.error(f"Error sending pong: {e}")
                    
                else:
                    try:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Unknown action: {action}",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    except Exception as e:
                        logger.error(f"Error sending error message: {e}")
            
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in WebSocket message: {e}")
                try:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Invalid JSON format",
                        "timestamp": datetime.utcnow().isoformat()
                    })
                except:
                    pass  # Connection may be closed
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}", exc_info=True)
                try:
                    await websocket.send_json({
                        "type": "error",
                        "message": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                except:
                    pass  # Connection may be closed
    
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected normally")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
        except:
            pass  # Connection may already be closed
    finally:
        ws_manager.disconnect(websocket)
        # Update subscriptions after disconnect
        if HAS_BINANCE_WS:
            try:
                await ws_manager.update_binance_subscriptions()
            except Exception as e:
                logger.error(f"Error updating Binance subscriptions: {e}")


# TODO: Implement actual streaming from exchange WebSockets
# This would require:
# 1. Connect to exchange WebSockets (Binance, Polygon, etc.) internally
# 2. Stream data to subscribed clients
# 3. Handle reconnections and errors
# 4. Support multiple providers
