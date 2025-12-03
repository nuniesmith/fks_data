# FKS Data

Ingests, validates, stores, and serves market data & derived datasets.

**Port**: 8003  
**Framework**: Python 3.12 + FastAPI  
**Role**: Market data ingestion, validation, storage, and serving

> 📢 **Update**: This service now includes all functionality previously in `fks_data_ingestion`, including NewsAPI integration. The `data_ingestion` service is deprecated.

## 🎯 Purpose

FKS Data is the central data service for the FKS Trading Platform. It provides:

- **Data Ingestion**: Multi-source market data collection (Binance, Polygon.io, Massive.com Futures, Yahoo, NewsAPI)
- **News Collection**: Financial news from NewsAPI for sentiment analysis
- **Data Validation**: Quality checks and normalization
- **Data Storage**: TimescaleDB for time-series data
- **Data Serving**: REST API for querying market data
- **Adapter Layer**: Unified API adapter with rate limiting and retries

**Critical Principle**: All FKS services query fks_data for market data. NO service should query exchanges directly for market data.

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐
│  Exchanges  │────▶│  fks_data    │
│ (Binance,   │     │  (Adapter    │
│  Polygon,   │     │   Layer)     │
│  NewsAPI)   │     └──────┬───────┘
└─────────────┘            │
                           ▼
                    ┌─────────────┐
                    │ TimescaleDB │
                    │  (Storage)  │
                    └──────┬───────┘
                           │
                           ▼
              ┌────────────┴────────────┐
              │                         │
       ┌──────┴──────┐         ┌───────┴───────┐
       │ fks_feature │         │    fks_app    │
       │ _engineering│         │    fks_ai     │
       │  (Features) │         │  (Consumers)  │
       └─────────────┘         └───────────────┘
```

**Key Components**:
- **Adapter Layer**: Unified API adapter with rate limiting, retries, exponential backoff
- **Providers**: Exchange-specific data providers (Binance, Polygon.io, Massive.com Futures, Yahoo, NewsAPI)
- **Pipelines**: Data transformation and enrichment
- **Validation**: Data quality checks
- **Store**: Persistence abstraction layer

## 🚀 Quick Start

### Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install .[postgres,redis]

# Run service
python -m fks_data.main

# Or using uvicorn
uvicorn src.main:app --reload --host 0.0.0.0 --port 8003
```

### Docker

```bash
# Build and run
docker-compose up --build

# Or using the unified start script
cd /home/jordan/Documents/code/fks
./start.sh --type compose
```

### Kubernetes

```bash
# Deploy to Kubernetes
cd /home/jordan/Documents/code/fks
./start.sh --type k8s
```

## 📡 API Endpoints

### Health Checks

- `GET /health` - Health check
- `GET /ready` - Readiness check (checks database connection)
- `GET /live` - Liveness probe

### Market Data

- `GET /api/v1/data/price?symbol={symbol}` - Get current price
- `GET /api/v1/data/ohlcv?symbol={symbol}&interval={interval}` - Get OHLCV data
- `GET /api/v1/data/providers` - List available data providers

### Massive.com Futures API ✅

**Status**: Fully integrated and available (service migrated to FastAPI)

The Massive.com Futures API (formerly Polygon.io Futures) provides comprehensive futures market data including contracts, products, schedules, aggregates, trades, quotes, and real-time WebSocket streams.

**Available Endpoints**:

- `GET /api/v1/futures/contracts` - List futures contracts (with filtering)
  - Query params: `product_code`, `first_trade_date`, `last_trade_date`, `as_of`, `active`, `type`, `limit`, `sort`
- `GET /api/v1/futures/contracts/{ticker}` - Get contract details
- `GET /api/v1/futures/products` - List all futures products
- `GET /api/v1/futures/products/{product_code}` - Get product details
- `GET /api/v1/futures/products/{product_code}/schedules` - Get product-specific trading schedules
- `GET /api/v1/futures/schedules` - Get all trading schedules
- `GET /api/v1/futures/aggs/{ticker}` - Get aggregate bars (OHLC)
  - Query params: `resolution` (1min, 5min, 1hour, 1day), `window_start`, `limit`, `sort`
- `GET /api/v1/futures/trades/{ticker}` - Get trades
  - Query params: `timestamp`, `session_end_date`, `limit`, `sort`
- `GET /api/v1/futures/quotes/{ticker}` - Get quotes
  - Query params: `timestamp`, `session_end_date`, `limit`, `sort`
- `GET /api/v1/futures/market-status` - Get current market status
- `GET /api/v1/futures/exchanges` - List supported exchanges
- `WS /api/v1/futures/ws` - WebSocket endpoint for real-time data streams

**API Key Configuration**:

The Massive.com Futures API requires an API key. Configure it via:

1. **Environment Variable** (recommended):
   ```bash
   export MASSIVE_API_KEY="your_futures_beta_key"
   # Or use legacy name:
   export POLYGON_API_KEY="your_futures_beta_key"
   export FKS_MASSIVE_API_KEY="your_futures_beta_key"
   ```

2. **Web Interface**:
   - Navigate to Settings > API Keys in fks_web
   - Add new API key with provider "Massive.com (Futures)"
   - Enter your Futures Beta API key

**Example Usage**:
```bash
# Get contracts for ES (E-mini S&P 500)
curl "http://localhost:8003/api/v1/futures/contracts?product_code=ES&limit=10"

# Get aggregate bars (1-minute resolution)
curl "http://localhost:8003/api/v1/futures/aggs/ESU0?resolution=1min&limit=100"

# Get all products
curl "http://localhost:8003/api/v1/futures/products"

# Get market status
curl "http://localhost:8003/api/v1/futures/market-status"
```

**WebSocket Usage**:
```python
import asyncio
import websockets
import json

async def subscribe_futures():
    uri = "ws://localhost:8003/api/v1/futures/ws"
    async with websockets.connect(uri) as websocket:
        # Subscribe to trades for ESU0
        await websocket.send(json.dumps({
            "action": "subscribe",
            "params": {
                "ticker": "ESU0",
                "type": "trades"
            }
        }))
        
        # Receive real-time data
        async for message in websocket:
            data = json.loads(message)
            print(f"Received: {data}")
```

**For detailed documentation**, see:
- `docs/MASSIVE_FUTURES.md` - Complete API reference and integration guide
- `docs/MASSIVE_FUTURES_API_TEST_RESULTS.md` - Test results and endpoint verification

**Note**: Massive.com rebranded from Polygon.io Futures on October 30, 2025. The API endpoints and functionality remain the same.

### Adapter API

- `GET /adapters` - List available adapters
- `GET /adapters/{provider}/status` - Get adapter status
- `POST /adapters/{provider}/fetch` - Fetch data via adapter

### Information

- `GET /info` - Service information and API docs

## 🔧 Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=fks_data
SERVICE_PORT=8003
FKS_DATA_HOST=0.0.0.0
FKS_DATA_PORT=8003

# Database (TimescaleDB)
DATABASE_URL=postgresql://fks_user:password@db:5432/trading_db
DB_HOST=db
DB_PORT=5432
DB_NAME=trading_db
DB_USER=fks_user
DB_PASSWORD=your_password

# Adapter Layer Configuration
FKS_API_TIMEOUT=10.0                    # Global HTTP timeout (seconds)
FKS_BINANCE_TIMEOUT=5                    # Binance-specific timeout
FKS_POLYGON_TIMEOUT=10                   # Polygon-specific timeout
FKS_DEFAULT_RPS=5                        # Default rate limit (requests/sec)
FKS_BINANCE_RPS=10                       # Binance rate limit
FKS_POLYGON_RPS=2                        # Polygon rate limit
FKS_API_MAX_RETRIES=2                    # Retry attempts
FKS_API_BACKOFF_BASE=0.3                 # Exponential backoff base
FKS_API_BACKOFF_JITTER=0.25              # Jitter upper bound

# Provider API Keys
POLYGON_API_KEY=your-polygon-api-key              # Legacy name (still supported)
FKS_POLYGON_API_KEY=your-polygon-api-key          # Alternative name
MASSIVE_API_KEY=your-massive-futures-api-key      # Massive.com Futures API key (recommended)
FKS_MASSIVE_API_KEY=your-massive-futures-api-key  # Alternative name for Massive.com
BINANCE_API_KEY=your-binance-api-key              # Optional (public data doesn't require key)

# Logging
FKS_JSON_LOGS=1                          # Enable JSON structured logs
LOG_LEVEL=INFO
```

### Configuration Files

- `src/framework/config/constants.py` - Configuration constants
- `config.yaml` - YAML configuration (if used)

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Test adapter layer
pytest tests/test_adapters_* -v

# Test manager integration
pytest tests/test_manager_adapter_integration.py -v

# With coverage
pytest tests/ --cov=src --cov-report=html

# Smoke test
pytest tests/ -k smoke -v
```

**Test Status**: 15 passed, 2 skipped (Week 2 completion validated)

## 🐳 Docker

### Build

```bash
docker build -t nuniesmith/fks:data-latest .
```

### Run

```bash
docker run -p 8003:8003 \
  -e DATABASE_URL=postgresql://fks_user:password@db:5432/trading_db \
  -e POLYGON_API_KEY=your-api-key \
  nuniesmith/fks:data-latest
```

### Docker Compose

```yaml
services:
  fks_data:
    build: .
    image: nuniesmith/fks:data-latest
    ports:
      - "8003:8003"
    environment:
      - DATABASE_URL=postgresql://fks_user:password@db:5432/trading_db
      - POLYGON_API_KEY=${POLYGON_API_KEY}
    depends_on:
      - db
```

## ☸️ Kubernetes

### Deployment

```bash
# Deploy using Helm
cd repo/main/k8s/charts/fks-platform
helm install fks-platform . -n fks-trading

# Or using manifests
kubectl apply -f repo/main/k8s/manifests/all-services.yaml -n fks-trading
```

### Health Checks

Kubernetes probes:
- **Liveness**: `GET /live`
- **Readiness**: `GET /ready` (checks database connection)

## 📚 Documentation

- [API Documentation](docs/API.md) - Complete API reference
- [Deployment Guide](docs/DEPLOYMENT.md) - Deployment instructions
- [Adapter Layer Guide](docs/ADAPTERS.md) - Adapter usage

## 🔗 Integration

### Dependencies

- **TimescaleDB/PostgreSQL**: Time-series data storage
- **Redis**: Caching (optional)
- **Exchange APIs**: Binance, Polygon, Yahoo Finance

### Consumers

- **fks_app**: Trading strategies and signals
- **fks_ai**: ML model training and inference
- **fks_web**: Dashboard data display

### Adapter Usage

```python
from fks_data.adapters import get_adapter

# Get adapter
binance = get_adapter("binance")

# Fetch data
bars = binance.fetch(symbol="BTCUSDT", interval="1m", limit=100)

# Or via DataManager
from src.manager import DataManager
dm = DataManager()
bars = dm.fetch_market_data("binance", symbol="BTCUSDT", interval="5m", limit=200)
```

**Canonical Normalized Format**:
```text
ts (unix seconds), open, high, low, close, volume
```

## 📊 Monitoring

### Health Check Endpoints

- `GET /health` - Basic health status
- `GET /ready` - Readiness (checks database connection)
- `GET /live` - Liveness (process alive)

### Metrics

- Request count and latency
- Adapter status and rate limit usage
- Database connection pool status
- Data ingestion rates

### Logging

- Structured JSON logging (when `FKS_JSON_LOGS=1`)
- Adapter request/response logging
- Error tracking and retry attempts

## 🛠️ Development

### Setup

```bash
# Clone repository
git clone https://github.com/nuniesmith/fks_data.git
cd fks_data

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install .[postgres,redis,dev]

# Run tests
pytest tests/ -v
```

### Code Structure

```
repo/data/
├── src/
│   ├── adapters/           # Adapter layer
│   │   ├── base.py         # Base APIAdapter
│   │   ├── binance.py      # Binance adapter
│   │   └── polygon.py      # Polygon adapter
│   ├── providers/          # Legacy providers
│   │   ├── binance.py      # Binance provider
│   │   └── polygon.py      # Polygon provider
│   ├── pipelines/          # Data pipelines
│   ├── validation.py       # Data validation
│   ├── store.py            # Persistence abstraction
│   ├── manager.py          # DataManager facade
│   └── main.py             # FastAPI application
├── tests/                   # Test suite
│   ├── test_adapters_*     # Adapter tests
│   └── test_manager_*      # Manager tests
├── Dockerfile              # Container definition
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

### Adapter Layer Features

- **Structured Logging**: JSON logs for production
- **Rate Limiting**: Per-provider rate limits
- **Retries**: Exponential backoff with jitter
- **Timeout Handling**: Configurable timeouts
- **Error Handling**: Graceful degradation

### Contributing

1. Follow Python best practices (PEP 8)
2. Write tests for new adapters
3. Document adapter-specific configuration
4. Update adapter registry
5. Test with real API keys (use testnet when possible)

## 🔄 Data Flow

1. **Ingestion**: Adapters fetch data from exchanges
2. **Validation**: Data quality checks
3. **Normalization**: Convert to canonical format
4. **Storage**: Persist to TimescaleDB
5. **Serving**: REST API serves data to consumers

## 🐛 Troubleshooting

### Adapter Connection Issues

- Verify API keys are set correctly
- Check rate limits (may need to reduce RPS)
- Review timeout settings
- Check exchange status pages

### Database Connection Issues

```bash
# Check database connectivity
psql $DATABASE_URL

# Verify TimescaleDB extension
psql $DATABASE_URL -c "SELECT * FROM pg_extension WHERE extname = 'timescaledb';"
```

### Rate Limit Exceeded

- Reduce `FKS_<PROVIDER>_RPS` values
- Increase `FKS_API_BACKOFF_BASE`
- Implement request batching
- Use WebSocket feeds when available

---

**Repository**: [nuniesmith/fks_data](https://github.com/nuniesmith/fks_data)  
**Docker Image**: `nuniesmith/fks:data-latest`  
**Status**: Active
