"""
Binance WebSocket client for real-time market data streaming.

Connects to Binance WebSocket streams and forwards kline (OHLCV) data.
Used internally by fks_data service to stream data to clients.
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Set, Callable, Any
from datetime import datetime

try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    WebSocketClientProtocol = None

logger = logging.getLogger(__name__)


class BinanceWebSocketClient:
    """WebSocket client for Binance kline streams.
    
    Connects to Binance WebSocket and streams OHLCV data.
    Supports multiple symbols and timeframes.
    """
    
    WS_URL = "wss://stream.binance.com:9443/stream"
    
    # Timeframe mapping: our format -> Binance format
    TIMEFRAME_MAP = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h",
        "6h": "6h",
        "8h": "8h",
        "12h": "12h",
        "1d": "1d",
        "3d": "3d",
        "1w": "1w",
        "1M": "1M"
    }
    
    def __init__(
        self,
        on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        """Initialize Binance WebSocket client.
        
        Args:
            on_message: Async callback for received messages (can be async or sync)
            on_error: Callback for errors
        """
        if not HAS_WEBSOCKETS:
            raise ImportError("websockets library required. Install with: pip install websockets")
        
        self.on_message = on_message
        self.on_error = on_error
        self.ws: Optional[WebSocketClientProtocol] = None
        self.connected = False
        self.running = False
        
        # Track subscriptions: symbol_timeframe -> stream_name
        self.subscriptions: Dict[str, str] = {}  # "BTCUSDT_1h" -> "btcusdt@kline_1h"
        self.stream_names: Set[str] = set()
    
    def _convert_timeframe(self, timeframe: str) -> str:
        """Convert our timeframe format to Binance format."""
        return self.TIMEFRAME_MAP.get(timeframe, "1h")
    
    def _build_stream_name(self, symbol: str, timeframe: str) -> str:
        """Build Binance stream name for kline."""
        symbol_lower = symbol.lower()
        binance_tf = self._convert_timeframe(timeframe)
        return f"{symbol_lower}@kline_{binance_tf}"
    
    async def connect(self) -> None:
        """Connect to Binance WebSocket."""
        if self.connected:
            logger.warning("Already connected to Binance WebSocket")
            return
        
        try:
            self.ws = await websockets.connect(self.WS_URL)
            self.connected = True
            self.running = True
            logger.info("Connected to Binance WebSocket")
        except Exception as e:
            logger.error(f"Failed to connect to Binance WebSocket: {e}")
            if self.on_error:
                self.on_error(e)
            raise
    
    async def subscribe(self, symbols: List[str], timeframes: List[str]) -> None:
        """Subscribe to kline streams for symbols and timeframes.
        
        Args:
            symbols: List of symbols (e.g., ["BTCUSDT", "ETHUSDT"])
            timeframes: List of timeframes (e.g., ["1m", "5m", "1h"])
        """
        if not self.connected or not self.ws:
            raise RuntimeError("Not connected to Binance WebSocket")
        
        # Build stream names
        new_streams = []
        for symbol in symbols:
            symbol_upper = symbol.upper()
            for timeframe in timeframes:
                stream_name = self._build_stream_name(symbol_upper, timeframe)
                key = f"{symbol_upper}_{timeframe}"
                
                # Add to subscriptions if not already subscribed
                if key not in self.subscriptions:
                    self.subscriptions[key] = stream_name
                    new_streams.append(stream_name)
                    self.stream_names.add(stream_name)
        
        if not new_streams:
            logger.debug("No new streams to subscribe to")
            return
        
        # Binance requires subscribing via stream names in URL or message
        # For multiple streams, we need to use the combined stream URL
        # Format: wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m
        
        # Reconnect with new streams
        await self.disconnect()
        
        # Build combined stream URL
        stream_names = "/".join(self.stream_names)
        combined_url = f"{self.WS_URL}?streams={stream_names}"
        
        try:
            self.ws = await websockets.connect(combined_url)
            self.connected = True
            self.running = True
            logger.info(f"Subscribed to {len(new_streams)} new streams (total: {len(self.stream_names)})")
        except Exception as e:
            logger.error(f"Failed to subscribe to Binance streams: {e}")
            if self.on_error:
                self.on_error(e)
            raise
    
    async def unsubscribe(self, symbols: Optional[List[str]] = None, timeframes: Optional[List[str]] = None) -> None:
        """Unsubscribe from streams.
        
        Args:
            symbols: Symbols to unsubscribe (None = all)
            timeframes: Timeframes to unsubscribe (None = all)
        """
        if not symbols:
            # Unsubscribe from all
            self.subscriptions.clear()
            self.stream_names.clear()
        else:
            # Unsubscribe from specific symbols/timeframes
            symbols_upper = [s.upper() for s in symbols]
            for key in list(self.subscriptions.keys()):
                symbol, tf = key.split("_", 1)
                if symbol in symbols_upper:
                    if not timeframes or tf in timeframes:
                        stream_name = self.subscriptions.pop(key)
                        self.stream_names.discard(stream_name)
        
        # Reconnect with updated streams
        if self.stream_names:
            await self.disconnect()
            stream_names = "/".join(self.stream_names)
            combined_url = f"{self.WS_URL}?streams={stream_names}"
            try:
                self.ws = await websockets.connect(combined_url)
                self.connected = True
                self.running = True
                logger.info(f"Unsubscribed. Remaining streams: {len(self.stream_names)}")
            except Exception as e:
                logger.error(f"Failed to reconnect after unsubscribe: {e}")
                if self.on_error:
                    self.on_error(e)
        else:
            await self.disconnect()
    
    async def listen(self):
        """Listen for messages and yield them."""
        if not self.connected or not self.ws:
            raise RuntimeError("Not connected to Binance WebSocket")
        
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    
                    # Binance sends: {"stream": "btcusdt@kline_1h", "data": {...}}
                    if "stream" in data and "data" in data:
                        stream = data["stream"]
                        kline_data = data["data"]
                        
                        # Parse stream name to get symbol and timeframe
                        if "@kline_" in stream:
                            symbol_tf = stream.split("@kline_")[0]
                            symbol = symbol_tf.upper()
                            binance_tf = stream.split("@kline_")[1]
                            
                            # Convert back to our timeframe format
                            timeframe = None
                            for our_tf, bin_tf in self.TIMEFRAME_MAP.items():
                                if bin_tf == binance_tf:
                                    timeframe = our_tf
                                    break
                            
                            if timeframe:
                                # Normalize kline data
                                normalized = self._normalize_kline(kline_data, symbol, timeframe)
                                
                                if self.on_message:
                                    # Call callback (can be async or sync)
                                    if asyncio.iscoroutinefunction(self.on_message):
                                        await self.on_message(normalized)
                                    else:
                                        self.on_message(normalized)
                                
                                yield normalized
                
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON from Binance WebSocket: {e}")
                except Exception as e:
                    logger.error(f"Error processing Binance WebSocket message: {e}")
                    if self.on_error:
                        self.on_error(e)
        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Binance WebSocket connection closed")
            self.connected = False
        except Exception as e:
            logger.error(f"Error in Binance WebSocket listen loop: {e}")
            self.connected = False
            if self.on_error:
                self.on_error(e)
    
    def _normalize_kline(self, kline_data: Dict[str, Any], symbol: str, timeframe: str) -> Dict[str, Any]:
        """Normalize Binance kline data to our format.
        
        Binance kline format:
        {
            "e": "kline",
            "E": 1234567890,
            "s": "BTCUSDT",
            "k": {
                "t": 1234567890000,  # Open time (ms)
                "T": 1234567895999,  # Close time (ms)
                "s": "BTCUSDT",
                "i": "1h",
                "o": "90000",
                "h": "91000",
                "l": "89000",
                "c": "90500",
                "v": "1000",
                "x": true  # Is closed
            }
        }
        
        Our format:
        {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "ts": 1234567890,  # Unix seconds
            "open": 90000.0,
            "high": 91000.0,
            "low": 89000.0,
            "close": 90500.0,
            "volume": 1000.0,
            "is_closed": true
        }
        """
        k = kline_data.get("k", {})
        
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "ts": int(k.get("t", 0) / 1000),  # Convert ms to seconds
            "open": float(k.get("o", 0)),
            "high": float(k.get("h", 0)),
            "low": float(k.get("l", 0)),
            "close": float(k.get("c", 0)),
            "volume": float(k.get("v", 0)),
            "is_closed": k.get("x", False),
            "provider": "binance",
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def disconnect(self) -> None:
        """Disconnect from Binance WebSocket."""
        self.running = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.error(f"Error closing Binance WebSocket: {e}")
            finally:
                self.ws = None
                self.connected = False
                logger.info("Disconnected from Binance WebSocket")


__all__ = ["BinanceWebSocketClient"]
