# FKS Data Service

Ingests, validates, stores, and serves market data & derived datasets.

## Components

- Providers (`providers/`): external data sources (e.g., Yahoo, Binance)
- Pipelines (`pipelines/`): transformation & enrichment
- Validation (`validation.py`): data quality checks
- Store (`store.py`): persistence abstraction
- Splitting (`splitting.py`): dataset partition logic

### Week 2 Adapter Layer (Unified External API Access)

Added during Week 2 to standardize outbound market data fetches:

- `fks_data.adapters.base.APIAdapter` supplies structured logging, rate limiting, retries with exponential backoff + jitter, and env‑driven configuration.
- Concrete adapters: `BinanceAdapter`, `PolygonAdapter` (registered via `fks_data.adapters.get_adapter`).
- Legacy provider functions in `providers/binance.py` and `providers/polygon.py` now delegate to adapters (backward compatible: map `ts` -> `time`).
- `DataManager.fetch_market_data(provider, **kwargs)` offers a façade entrypoint for services/tools already using `DataManager`.

Canonical normalized row keys emitted by adapters:

```text
ts (unix seconds), open, high, low, close, volume
```

### Environment Variables (Adapter Layer)

| Variable | Purpose | Example Default |
|----------|---------|-----------------|
| `FKS_API_TIMEOUT` | Global HTTP timeout (seconds) | 10.0 |
| `FKS_<PROVIDER>_TIMEOUT` | Provider specific timeout override | `FKS_BINANCE_TIMEOUT=5` |
| `FKS_<PROVIDER>_RPS` | Per‑provider rate limit (requests/sec) | `FKS_POLYGON_RPS=2` |
| `FKS_DEFAULT_RPS` | Fallback rate limit if per‑provider not set | 5 |
| `FKS_API_MAX_RETRIES` | Retry attempts (total = value + 1 initial) | 2 |
| `FKS_API_BACKOFF_BASE` | Base seconds for exponential backoff | 0.3 |
| `FKS_API_BACKOFF_JITTER` | Added random jitter upper bound | 0.25 |
| `POLYGON_API_KEY` / `FKS_POLYGON_API_KEY` | Polygon auth bearer token | (none) |
| `FKS_JSON_LOGS` | Enable JSON structured logs via shared logger | `1` |

Usage example:

```python
from fks_data.adapters import get_adapter
binance = get_adapter("binance")
bars = binance.fetch(symbol="BTCUSDT", interval="1m", limit=100)
```

Or via `DataManager`:

```python
from src.manager import DataManager
dm = DataManager()
bars = dm.fetch_market_data("binance", symbol="BTCUSDT", interval="5m", limit=200)
```

Testing: see `tests/test_adapters_*` and `tests/test_manager_adapter_integration.py` for normalization, retries, logging JSON, and manager façade coverage.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install .[postgres,redis]
python -m fks_data.main
```

## Environment

Configure credentials via exported env vars or `.env` loaded upstream.

## Smoke Test

(Placeholder) Add tests exercising a sample provider and validation.

Week 2 completion: adapter layer + multi‑provider tests (15 passed, 2 skipped) validated.
