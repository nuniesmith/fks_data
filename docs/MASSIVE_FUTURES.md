# Massive.com Futures Integration

This document describes the Massive.com (formerly Polygon.io) Futures API integration for fks_data.

## Overview

The integration provides both REST API and WebSocket support for U.S. futures market data from Massive.com, including:
- Contracts (list, details)
- Products (list, details)
- Trading schedules
- Aggregate bars (OHLC)
- Trades (tick-level)
- Quotes (bid/ask)
- Market status
- Exchanges

## Configuration

### Option 1: Web Interface (Recommended - Secure)

1. Log into the FKS web interface
2. Navigate to **API Keys** (requires staff/superuser access)
3. Click **Create New API Key**
4. Fill in the form:
   - **Name**: e.g., `massive_futures_beta`
   - **Provider**: Select `Massive.com (Futures)`
   - **Description**: e.g., `Futures Beta API key for testing`
   - **API Key**: Enter your Massive.com API key (will be encrypted)
   - **Global**: Check if this key should be available to all services
   - **Active**: Check to enable the key
5. Click **Save**

The API key will be encrypted and stored securely in the database. The adapter will automatically use it when available.

### Option 2: Environment Variables

Set one of the following environment variables for API authentication:
- `MASSIVE_API_KEY` (preferred)
- `FKS_MASSIVE_API_KEY`
- `POLYGON_API_KEY` (fallback for compatibility)

**Note**: Environment variables take precedence over database-stored keys for security and flexibility.

## REST API Usage

### Using the Adapter Directly

```python
from adapters import get_adapter

# Get the adapter
adapter = get_adapter("massive_futures")

# Fetch aggregate bars (OHLC)
result = adapter.fetch(
    endpoint_type="aggs",
    ticker="ESU0",  # E-mini S&P 500 futures
    resolution="1min",
    limit=1000,
    sort="window_start.desc"
)

# Fetch contracts
result = adapter.fetch(
    endpoint_type="contracts",
    product_code="ES",
    active="true",
    limit=100
)

# Fetch trades
result = adapter.fetch(
    endpoint_type="trades",
    ticker="GCJ5",  # Gold futures
    limit=1000
)
```

### Available Endpoint Types

- `contracts` - List all contracts
- `contract` - Get specific contract details
- `products` - List all products
- `product` - Get specific product details
- `schedules` - Get all schedules
- `product_schedules` - Get schedules for a product
- `aggs` - Aggregate bars (OHLC)
- `trades` - Tick-level trades
- `quotes` - Bid/ask quotes
- `market_status` - Current market status
- `exchanges` - List exchanges

## FastAPI Routes

The integration includes FastAPI routes at `/api/v1/futures/`:

### Examples

```bash
# Get contracts
GET /api/v1/futures/contracts?product_code=ES&active=true

# Get contract details
GET /api/v1/futures/contracts/ESU0

# Get aggregate bars
GET /api/v1/futures/aggs/ESU0?resolution=1min&limit=1000

# Get trades
GET /api/v1/futures/trades/GCJ5?limit=1000

# Get quotes
GET /api/v1/futures/quotes/ESU0?limit=1000

# Get market status
GET /api/v1/futures/market-status?product_code=ES
```

## WebSocket Usage

### Using the WebSocket Client

```python
import asyncio
from adapters.massive_futures_ws import MassiveFuturesWebSocket

async def main():
    async with MassiveFuturesWebSocket() as ws:
        # Subscribe to trades
        await ws.subscribe_trades(["ESU0", "GCJ5"])
        
        # Subscribe to quotes
        await ws.subscribe_quotes(["ESU0"])
        
        # Subscribe to aggregate bars
        await ws.subscribe_aggregates(["ESU0"], resolution="1min")
        
        # Listen for messages
        async for message in ws.listen():
            print(message)

asyncio.run(main())
```

### WebSocket Endpoint

Connect to `/api/v1/futures/ws` via WebSocket:

```javascript
const ws = new WebSocket('ws://localhost:8003/api/v1/futures/ws');

ws.onmessage = (event) => {
    const message = JSON.parse(event.data);
    console.log(message);
};

// Subscribe to trades
ws.send(JSON.stringify({
    action: "subscribe",
    type: "trades",
    tickers: ["ESU0", "GCJ5"]
}));

// Subscribe to quotes
ws.send(JSON.stringify({
    action: "subscribe",
    type: "quotes",
    tickers: ["ESU0"]
}));

// Subscribe to aggregates
ws.send(JSON.stringify({
    action: "subscribe",
    type: "aggregates",
    tickers: ["ESU0"],
    resolution: "1min"
}));
```

## Data Normalization

All responses are normalized to a canonical format:

### Aggregate Bars
```json
{
    "ts": 1738627200,
    "open": 2849.8,
    "high": 2877.1,
    "low": 2837.4,
    "close": 2874.2,
    "volume": 133072,
    "transactions": 74223,
    "dollar_volume": 380560636.01,
    "settlement_price": 2875.8,
    "session_end_date": "2025-02-04",
    "ticker": "GCJ5"
}
```

### Trades
```json
{
    "ts": 1734484219,
    "price": 605400,
    "size": 12,
    "ticker": "ESZ4",
    "session_end_date": "2024-12-17"
}
```

### Quotes
```json
{
    "ts": 1734476400,
    "bid_price": 604075,
    "bid_size": 6,
    "ask_price": 604100,
    "ask_size": 2,
    "ticker": "ESZ4",
    "session_end_date": "2024-12-17"
}
```

## Caching

The adapter supports Redis caching with a default TTL of 5 minutes (300 seconds). Cache keys are built from request parameters to ensure proper cache invalidation.

## Rate Limiting

Default rate limit: 4 requests per second. Can be overridden via:
- `FKS_MASSIVE_FUTURES_RPS` environment variable
- `FKS_DEFAULT_RPS` environment variable

## Error Handling

All errors are wrapped in `DataFetchError` exceptions with descriptive messages. The FastAPI routes return appropriate HTTP status codes (400, 404, 500, etc.).

## Testing

You have Futures Beta access, so you can test all endpoints. Example tickers:
- `ESU0` - E-mini S&P 500 futures
- `GCJ5` - Gold futures (April 2025)
- `NQU0` - E-mini NASDAQ-100 futures

## Notes

- All timestamps are normalized to Unix seconds (not nanoseconds)
- All dates use Central Time (CT) as per futures exchange standards
- WebSocket URL may need to be updated when Massive.com releases their WebSocket endpoint
- The adapter supports pagination via `next_url` in responses
