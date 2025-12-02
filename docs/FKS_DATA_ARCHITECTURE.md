# FKS Data Service Architecture

**Service**: fks_data  
**Port**: 8003  
**Framework**: FastAPI (migrated from Flask)  
**Version**: 2.0.0  
**Last Updated**: 2025-12-01

## Overview

FKS Data is the central market data service for the FKS Trading Platform. It provides a unified interface for ingesting, validating, storing, and serving market data from multiple sources.

**Critical Principle**: All FKS services query fks_data for market data. NO service should query exchanges directly.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    External Data Sources                     │
├─────────────────────────────────────────────────────────────┤
│  Binance  │  Polygon.io  │  Massive.com  │  Yahoo  │  ...  │
└─────┬──────┴───────┬───────┴───────┬───────┴────┬───┴───────┘
      │              │               │            │
      └──────────────┴───────────────┴────────────┘
                      │
                      ▼
      ┌───────────────────────────────────────┐
      │         Adapter Layer                  │
      │  ┌─────────────────────────────────┐  │
      │  │  Base APIAdapter                │  │
      │  │  - Rate limiting                │  │
      │  │  - Retries with backoff         │  │
      │  │  - Error handling              │  │
      │  │  - Request/response logging     │  │
      │  └─────────────────────────────────┘  │
      │                                        │
      │  ┌──────────┐  ┌──────────┐  ┌──────┐│
      │  │ Binance  │  │ Polygon  │  │Massive││
      │  │ Adapter  │  │ Adapter  │  │Futures││
      │  └──────────┘  └──────────┘  └──────┘│
      │  ┌──────────┐  ┌──────────┐  ┌──────┐│
      │  │ CoinMCap │  │CoinGecko │  │EODHD ││
      │  │ Adapter  │  │ Adapter  │  │Adapter││
      │  └──────────┘  └──────────┘  └──────┘│
      └───────────────────────────────────────┘
                      │
                      ▼
      ┌───────────────────────────────────────┐
      │      Data Processing Layer             │
      │  - Validation                          │
      │  - Normalization                       │
      │  - Quality scoring                     │
      │  - Enrichment                          │
      └───────────────────────────────────────┘
                      │
                      ▼
      ┌───────────────────────────────────────┐
      │         Storage Layer                  │
      │  ┌─────────────────────────────────┐  │
      │  │      TimescaleDB                 │  │
      │  │  (Time-series optimized)         │  │
      │  └─────────────────────────────────┘  │
      │  ┌─────────────────────────────────┐  │
      │  │      Redis Cache                 │  │
      │  │  (Fast access, rate limiting)    │  │
      │  └─────────────────────────────────┘  │
      └───────────────────────────────────────┘
                      │
                      ▼
      ┌───────────────────────────────────────┐
      │         API Layer (FastAPI)            │
      │  ┌─────────────────────────────────┐  │
      │  │  REST Endpoints                 │  │
      │  │  - /api/v1/data/*               │  │
      │  │  - /api/v1/futures/*            │  │
      │  │  - /health                      │  │
      │  └─────────────────────────────────┘  │
      │  ┌─────────────────────────────────┐  │
      │  │  WebSocket Endpoints             │  │
      │  │  - /api/v1/futures/ws           │  │
      │  └─────────────────────────────────┘  │
      └───────────────────────────────────────┘
                      │
                      ▼
      ┌───────────────────────────────────────┐
      │         Consumer Services              │
      │  fks_app  │  fks_ai  │  fks_web  │... │
      └───────────────────────────────────────┘
```

## Key Components

### 1. Adapter Layer

**Location**: `src/adapters/`

The adapter layer provides a unified interface to multiple data providers. All adapters inherit from `APIAdapter` base class.

**Available Adapters**:

1. **BinanceAdapter** (`binance.py`)
   - Crypto exchange data
   - Real-time and historical OHLCV
   - WebSocket support

2. **PolygonAdapter** (`polygon.py`)
   - Stock market data (formerly Polygon.io)
   - Historical and real-time data
   - Aggregates, trades, quotes

3. **MassiveFuturesAdapter** (`massive_futures.py`)
   - Futures market data (formerly Polygon.io Futures)
   - Contracts, products, schedules
   - Aggregates, trades, quotes
   - WebSocket support (`massive_futures_ws.py`)

4. **CoinMarketCapAdapter** (`cmc.py`)
   - Cryptocurrency market data
   - Market cap, rankings, metadata

5. **CoinGeckoAdapter** (`coingecko.py`)
   - Cryptocurrency market data
   - Alternative to CoinMarketCap

6. **EODHDAdapter** (`eodhd.py`)
   - End-of-day historical data
   - Stock and crypto data

7. **AlphaVantageAdapter** (`alpha_vantage.py`)
   - Stock market data
   - Technical indicators

**Adapter Features**:
- Rate limiting (per-provider)
- Automatic retries with exponential backoff
- Request/response logging
- Error handling and graceful degradation
- Timeout management

### 2. Data Manager

**Location**: `src/manager.py`

The `DataManager` class provides a facade over the adapter factory, simplifying data fetching:

```python
from src.manager import DataManager

dm = DataManager()
data = dm.fetch_market_data("binance", symbol="BTCUSDT", interval="1m", limit=100)
```

### 3. Multi-Provider Manager

**Location**: `src/adapters/multi_provider_manager.py`

Manages multiple providers with failover logic. If one provider fails, automatically tries the next.

### 4. Data Processing

**Validation** (`src/validation.py`, `src/validators/`):
- Data quality checks
- Completeness validation
- Freshness monitoring
- Outlier detection
- Quality scoring

**Normalization**:
- Converts all data to canonical format
- Standard timestamp format (Unix seconds)
- Consistent field names

### 5. Storage

**TimescaleDB** (`src/database/`):
- Time-series optimized PostgreSQL
- Efficient storage and querying of OHLCV data
- Automatic data partitioning

**Redis** (`src/framework/cache/`):
- Caching layer for frequently accessed data
- Rate limiting state
- Session storage

### 6. API Layer

**FastAPI Application** (`src/main_fastapi.py`):
- RESTful API endpoints
- WebSocket support for real-time data
- OpenAPI documentation at `/docs`
- Health check endpoints

**Routes**:
- `src/api/routes/health.py` - Health checks
- `src/api/routes/data.py` - Market data endpoints
- `src/api/routes/massive_futures.py` - Futures-specific endpoints
- `src/api/routes/webhooks.py` - Webhook endpoints

### 7. Background Tasks

**Celery** (`src/celery_app.py`, `src/tasks/`):
- Scheduled data collection
- Batch processing
- Async data ingestion

## Data Flow

### 1. Data Ingestion Flow

```
External API → Adapter → Validation → Normalization → Storage → Cache
```

1. **Request**: Consumer service requests data via API
2. **Adapter Selection**: System selects appropriate adapter based on symbol/provider
3. **Fetch**: Adapter fetches data from external API (with rate limiting/retries)
4. **Validate**: Data quality checks performed
5. **Normalize**: Data converted to canonical format
6. **Store**: Data persisted to TimescaleDB
7. **Cache**: Frequently accessed data cached in Redis
8. **Response**: Data returned to consumer

### 2. Real-Time Data Flow

```
WebSocket → Adapter → Validation → Normalization → Storage → Broadcast
```

1. **Connection**: Client connects to WebSocket endpoint
2. **Subscription**: Client subscribes to specific symbols/streams
3. **Stream**: Adapter receives real-time updates
4. **Process**: Data validated and normalized
5. **Store**: Data persisted to database
6. **Broadcast**: Updates sent to all subscribed clients

## API Endpoints

### Health & Status

- `GET /health` - Basic health check
- `GET /ready` - Readiness check (database connection)
- `GET /live` - Liveness probe
- `GET /info` - Service information
- `GET /status` - Detailed status

### Market Data

- `GET /api/v1/data/price?symbol={symbol}` - Get current price
- `GET /api/v1/data/ohlcv?symbol={symbol}&interval={interval}` - Get OHLCV data
- `GET /api/v1/data/providers` - List available providers

### Futures Data (Massive.com)

- `GET /api/v1/futures/contracts` - List contracts
- `GET /api/v1/futures/products` - List products
- `GET /api/v1/futures/aggs/{ticker}` - Get aggregates
- `GET /api/v1/futures/trades/{ticker}` - Get trades
- `GET /api/v1/futures/quotes/{ticker}` - Get quotes
- `WS /api/v1/futures/ws` - WebSocket for real-time data

## Configuration

### Environment Variables

```bash
# Service
SERVICE_NAME=fks_data
SERVICE_PORT=8003
FKS_DATA_HOST=0.0.0.0

# Database
DATABASE_URL=postgresql://user:pass@host:5432/db
DB_HOST=db
DB_PORT=5432
DB_NAME=trading_db

# Redis
REDIS_URL=redis://host:6379/0

# API Keys
POLYGON_API_KEY=your-key
MASSIVE_API_KEY=your-key
BINANCE_API_KEY=your-key

# Rate Limiting
FKS_DEFAULT_RPS=5
FKS_BINANCE_RPS=10
FKS_POLYGON_RPS=2
```

## Dependencies

### Core Dependencies

- **FastAPI**: Web framework
- **Uvicorn**: ASGI server
- **httpx**: HTTP client for adapters
- **psycopg2**: PostgreSQL driver
- **redis**: Redis client
- **celery**: Background task processing
- **pandas**: Data manipulation
- **numpy**: Numerical operations

### Adapter-Specific

- **websockets**: WebSocket support (Massive.com Futures)
- **ccxt**: Crypto exchange library (optional)

## Testing

### Test Structure

- `tests/test_adapters_*.py` - Adapter tests
- `tests/test_manager_*.py` - DataManager tests
- `tests/test_quality_*.py` - Quality validation tests
- `tests/test_schema_*.py` - Schema validation tests

### Running Tests

```bash
pytest tests/ -v
pytest tests/test_adapters_* -v
pytest tests/ --cov=src --cov-report=html
```

## Deployment

### Docker

```bash
docker-compose up --build
```

### Kubernetes

```bash
kubectl apply -f k8s/manifests/
```

## Monitoring

### Metrics

- Request count and latency
- Adapter status and rate limit usage
- Database connection pool status
- Data ingestion rates
- Cache hit rates

### Logging

- Structured JSON logging (when `FKS_JSON_LOGS=1`)
- Adapter request/response logging
- Error tracking and retry attempts

## Future Enhancements

- [ ] GraphQL API for flexible queries
- [ ] More data providers (Kraken, KuCoin, etc.)
- [ ] Advanced caching strategies
- [ ] Data compression for storage
- [ ] Real-time alerting on data quality issues
- [ ] Machine learning for data quality prediction

---

**Related Documentation**:
- [README.md](../README.md) - Service overview and quick start
- [MASSIVE_FUTURES.md](./MASSIVE_FUTURES.md) - Futures API documentation
- [MASSIVE_FUTURES_API_TEST_RESULTS.md](./MASSIVE_FUTURES_API_TEST_RESULTS.md) - Test results
