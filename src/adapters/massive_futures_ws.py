"""Massive.com Futures WebSocket client for real-time market data.

Note: Massive.com is the new name for Polygon.io's futures division.
This client is specifically for U.S. futures market data.

Supports subscribing to real-time trades, quotes, and aggregate bars for futures contracts.

Env vars:
  MASSIVE_API_KEY or FKS_MASSIVE_API_KEY or POLYGON_API_KEY for authentication.
  API keys can also be managed via the web interface (encrypted storage).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Set

try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    WebSocketClientProtocol = None  # type: ignore

try:
    from shared_python.exceptions import DataFetchError  # type: ignore
except (ImportError, ModuleNotFoundError):
    class DataFetchError(Exception):
        """Fallback exception when shared_python is not available."""
        pass

from .base import get_env_any

logger = logging.getLogger(__name__)


class MassiveFuturesWebSocket:
    """WebSocket client for Massive.com Futures real-time data.
    
    Supports subscriptions to:
    - Trades: Real-time trade data
    - Quotes: Real-time bid/ask quotes
    - Aggregates: Real-time OHLC bars
    
    Usage:
        client = MassiveFuturesWebSocket()
        await client.connect()
        await client.subscribe_trades(["ESU0", "GCJ5"])
        await client.subscribe_quotes(["ESU0"])
        
        async for message in client.listen():
            print(message)
    """
    
    # Note: WebSocket URL may need to be updated when Massive.com releases their WebSocket endpoint
    # Check Massive.com documentation for the correct WebSocket URL
    WS_URL = "wss://socket.polygon.io/futures"  # TODO: Update to Massive.com WebSocket URL when available
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        """Initialize WebSocket client.
        
        Args:
            api_key: API key (defaults to env vars)
            on_message: Callback for received messages
            on_error: Callback for errors
        """
        self.api_key = api_key or get_env_any("MASSIVE_API_KEY", "FKS_MASSIVE_API_KEY", "POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("API key required. Set MASSIVE_API_KEY, FKS_MASSIVE_API_KEY, or POLYGON_API_KEY")
        
        self.on_message = on_message
        self.on_error = on_error
        self.ws: Optional[WebSocketClientProtocol] = None
        self.connected = False
        self.subscriptions: Set[str] = set()
        
        # Track subscriptions by type
        self.trade_subscriptions: Set[str] = set()
        self.quote_subscriptions: Set[str] = set()
        self.agg_subscriptions: Set[str] = set()
    
    async def connect(self) -> None:
        """Connect to WebSocket server."""
        if not HAS_WEBSOCKETS:
            raise DataFetchError("massive_futures_ws", "websockets library required. Install with: pip install websockets")
        
        if self.connected:
            logger.warning("Already connected")
            return
        
        try:
            # Authenticate with API key in connection URL or first message
            auth_url = f"{self.WS_URL}?apiKey={self.api_key}"
            self.ws = await websockets.connect(auth_url)  # type: ignore
            self.connected = True
            logger.info("Connected to Massive Futures WebSocket")
            
            # Send authentication message (if required by API)
            auth_msg = {
                "action": "auth",
                "params": self.api_key
            }
            await self.ws.send(json.dumps(auth_msg))
            
        except Exception as e:
            self.connected = False
            logger.error(f"Failed to connect to WebSocket: {e}")
            raise DataFetchError("massive_futures_ws", f"Connection failed: {e}") from e
    
    async def disconnect(self) -> None:
        """Disconnect from WebSocket server."""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self.ws = None
                self.connected = False
                self.subscriptions.clear()
                self.trade_subscriptions.clear()
                self.quote_subscriptions.clear()
                self.agg_subscriptions.clear()
                logger.info("Disconnected from Massive Futures WebSocket")
    
    async def subscribe_trades(self, tickers: List[str]) -> None:
        """Subscribe to real-time trades for given tickers.
        
        Args:
            tickers: List of futures contract tickers (e.g., ["ESU0", "GCJ5"])
        """
        if not self.connected or not self.ws:
            raise DataFetchError("massive_futures_ws", "Not connected. Call connect() first.")
        
        for ticker in tickers:
            self.trade_subscriptions.add(ticker)
            self.subscriptions.add(f"T.{ticker}")
        
        # Send subscription message
        # Format may vary - adjust based on actual API documentation
        subscribe_msg = {
            "action": "subscribe",
            "params": [f"T.{ticker}" for ticker in tickers]  # T. prefix for trades
        }
        await self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to trades for: {tickers}")
    
    async def subscribe_quotes(self, tickers: List[str]) -> None:
        """Subscribe to real-time quotes for given tickers.
        
        Args:
            tickers: List of futures contract tickers
        """
        if not self.connected or not self.ws:
            raise DataFetchError("massive_futures_ws", "Not connected. Call connect() first.")
        
        for ticker in tickers:
            self.quote_subscriptions.add(ticker)
            self.subscriptions.add(f"Q.{ticker}")
        
        subscribe_msg = {
            "action": "subscribe",
            "params": [f"Q.{ticker}" for ticker in tickers]  # Q. prefix for quotes
        }
        await self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to quotes for: {tickers}")
    
    async def subscribe_aggregates(self, tickers: List[str], resolution: str = "1min") -> None:
        """Subscribe to real-time aggregate bars for given tickers.
        
        Args:
            tickers: List of futures contract tickers
            resolution: Bar resolution (e.g., "1min", "5min", "1hour")
        """
        if not self.connected or not self.ws:
            raise DataFetchError("massive_futures_ws", "Not connected. Call connect() first.")
        
        for ticker in tickers:
            self.agg_subscriptions.add(ticker)
            self.subscriptions.add(f"A.{ticker}.{resolution}")
        
        subscribe_msg = {
            "action": "subscribe",
            "params": [f"A.{ticker}.{resolution}" for ticker in tickers]  # A. prefix for aggregates
        }
        await self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to aggregates ({resolution}) for: {tickers}")
    
    async def unsubscribe(self, tickers: List[str], data_type: str = "all") -> None:
        """Unsubscribe from data streams.
        
        Args:
            tickers: List of tickers to unsubscribe from
            data_type: "trades", "quotes", "aggregates", or "all"
        """
        if not self.connected or not self.ws:
            return
        
        params = []
        for ticker in tickers:
            if data_type in ["trades", "all"] and ticker in self.trade_subscriptions:
                params.append(f"T.{ticker}")
                self.trade_subscriptions.discard(ticker)
                self.subscriptions.discard(f"T.{ticker}")
            
            if data_type in ["quotes", "all"] and ticker in self.quote_subscriptions:
                params.append(f"Q.{ticker}")
                self.quote_subscriptions.discard(ticker)
                self.subscriptions.discard(f"Q.{ticker}")
            
            if data_type in ["aggregates", "all"]:
                # Remove all aggregate subscriptions for this ticker
                to_remove = [sub for sub in self.agg_subscriptions if sub.startswith(f"{ticker}.")]
                for sub in to_remove:
                    self.agg_subscriptions.discard(sub)
                    self.subscriptions.discard(f"A.{sub}")
        
        if params:
            unsubscribe_msg = {
                "action": "unsubscribe",
                "params": params
            }
            await self.ws.send(json.dumps(unsubscribe_msg))
            logger.info(f"Unsubscribed from {data_type} for: {tickers}")
    
    async def listen(self):
        """Async generator that yields messages from WebSocket.
        
        Yields:
            Dict containing normalized message data
        """
        if not self.connected or not self.ws:
            raise DataFetchError("massive_futures_ws", "Not connected. Call connect() first.")
        
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    normalized = self._normalize_message(data)
                    
                    if self.on_message:
                        try:
                            self.on_message(normalized)
                        except Exception as e:
                            logger.error(f"Error in on_message callback: {e}")
                            if self.on_error:
                                self.on_error(e)
                    
                    yield normalized
                
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse WebSocket message: {e}")
                    if self.on_error:
                        self.on_error(e)
                
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
                    if self.on_error:
                        self.on_error(e)
        
        except websockets.exceptions.ConnectionClosed:  # type: ignore
            logger.warning("WebSocket connection closed")
            self.connected = False
        except Exception as e:
            logger.error(f"WebSocket listen error: {e}")
            self.connected = False
            raise
    
    def _normalize_message(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize WebSocket message to canonical format.
        
        Args:
            raw: Raw message from WebSocket
            
        Returns:
            Normalized message dictionary
        """
        msg_type = raw.get("ev") or raw.get("event")  # Event type
        
        if msg_type == "status":
            # Status/control messages
            return {
                "type": "status",
                "message": raw.get("message", ""),
                "status": raw.get("status", ""),
            }
        
        elif msg_type in ["T", "trade"]:
            # Trade message
            timestamp = raw.get("t") or raw.get("timestamp", 0)
            if isinstance(timestamp, int) and timestamp > 1e15:
                timestamp = timestamp // 1_000_000_000
            
            return {
                "type": "trade",
                "ts": int(timestamp),
                "ticker": raw.get("sym") or raw.get("ticker", ""),
                "price": float(raw.get("p") or raw.get("price", 0)),
                "size": int(raw.get("s") or raw.get("size", 0)),
                "session_end_date": raw.get("session_end_date"),
            }
        
        elif msg_type in ["Q", "quote"]:
            # Quote message
            timestamp = raw.get("t") or raw.get("timestamp", 0)
            if isinstance(timestamp, int) and timestamp > 1e15:
                timestamp = timestamp // 1_000_000_000
            
            return {
                "type": "quote",
                "ts": int(timestamp),
                "ticker": raw.get("sym") or raw.get("ticker", ""),
                "bid_price": float(raw.get("bp") or raw.get("bid_price", 0)) if raw.get("bp") or raw.get("bid_price") else None,
                "bid_size": int(raw.get("bs") or raw.get("bid_size", 0)) if raw.get("bs") or raw.get("bid_size") else None,
                "ask_price": float(raw.get("ap") or raw.get("ask_price", 0)) if raw.get("ap") or raw.get("ask_price") else None,
                "ask_size": int(raw.get("as") or raw.get("ask_size", 0)) if raw.get("as") or raw.get("ask_size") else None,
                "session_end_date": raw.get("session_end_date"),
            }
        
        elif msg_type in ["A", "aggregate", "bar"]:
            # Aggregate bar message
            timestamp = raw.get("t") or raw.get("timestamp", 0) or raw.get("window_start", 0)
            if isinstance(timestamp, int) and timestamp > 1e15:
                timestamp = timestamp // 1_000_000_000
            
            return {
                "type": "aggregate",
                "ts": int(timestamp),
                "ticker": raw.get("sym") or raw.get("ticker", ""),
                "open": float(raw.get("o") or raw.get("open", 0)),
                "high": float(raw.get("h") or raw.get("high", 0)),
                "low": float(raw.get("l") or raw.get("low", 0)),
                "close": float(raw.get("c") or raw.get("close", 0)),
                "volume": int(raw.get("v") or raw.get("volume", 0)),
                "transactions": int(raw.get("n") or raw.get("transactions", 0)),
                "session_end_date": raw.get("session_end_date"),
            }
        
        else:
            # Unknown message type - return raw
            return {
                "type": "unknown",
                "raw": raw,
            }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


__all__ = ["MassiveFuturesWebSocket"]
