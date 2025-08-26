-- Migration 001: Initial OHLCV and dataset_splits tables (aligned with market_bar v1 schema)
CREATE TABLE IF NOT EXISTS ohlcv (
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (source, symbol, interval, ts)
);

CREATE TABLE IF NOT EXISTS dataset_splits (
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    split TEXT NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, symbol, interval, split)
);
