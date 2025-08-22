# FKS Data Service

Ingests, validates, stores, and serves market data & derived datasets.

## Components

- Providers (`providers/`): external data sources (e.g., Yahoo, Binance)
- Pipelines (`pipelines/`): transformation & enrichment
- Validation (`validation.py`): data quality checks
- Store (`store.py`): persistence abstraction
- Splitting (`splitting.py`): dataset partition logic

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
