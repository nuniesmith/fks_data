#!/bin/bash
# Initialize TimescaleDB database for fks_data

set -e

DB_HOST=${DB_HOST:-fks_data_db}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-trading_db}
DB_USER=${DB_USER:-fks_user}
DB_PASSWORD=${DB_PASSWORD:-fks_password}

echo "Initializing TimescaleDB database..."

# Wait for database to be ready
until PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c '\q' 2>/dev/null; do
  echo "Waiting for database to be ready..."
  sleep 2
done

echo "Database is ready. Creating TimescaleDB extension and tables..."

# Create TimescaleDB extension
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME <<EOF
-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create ohlcv table
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

-- Convert to hypertable if not already
SELECT create_hypertable('ohlcv', 'ts', if_not_exists => TRUE);

-- Create dataset_splits table
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

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_interval ON ohlcv(symbol, interval, ts DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_source_symbol ON ohlcv(source, symbol);

EOF

echo "Database initialization complete!"

