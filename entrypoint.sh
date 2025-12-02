#!/bin/bash
# Entrypoint script for fks_data

set -e

# Default values
SERVICE_NAME=${SERVICE_NAME:-fks_data}
SERVICE_PORT=${SERVICE_PORT:-8003}
HOST=${HOST:-0.0.0.0}

echo "Starting ${SERVICE_NAME} on ${HOST}:${SERVICE_PORT}"

# Wait for database to be ready (if DATABASE_URL is set)
if [ -n "$DATABASE_URL" ] || [ -n "$DB_HOST" ]; then
    echo "Waiting for database to be ready..."
    DB_HOST=${DB_HOST:-fks_data_db}
    DB_PORT=${DB_PORT:-5432}
    
    # Simple TCP connection check (works without psql)
    until timeout 2 bash -c "cat < /dev/null > /dev/tcp/$DB_HOST/$DB_PORT" 2>/dev/null; do
        echo "Database not ready, waiting..."
        sleep 2
    done
    echo "Database is ready!"
    
    # Initialize database schema (if Python script exists)
    if [ -f "scripts/init_schema.py" ] || [ -f "src/scripts/init_schema.py" ]; then
        echo "Initializing database schema..."
        python3 -m scripts.init_schema 2>/dev/null || python3 scripts/init_schema.py 2>/dev/null || echo "Warning: Schema initialization skipped (will be created on first use)"
    fi
fi

# Wait for Redis to be ready (if REDIS_URL is set)
if [ -n "$REDIS_URL" ]; then
    echo "Checking Redis connection..."
    # Extract host and port from REDIS_URL
    # Handle formats: redis://host:port, redis://:@host:port, redis://:@host:6379/0
    REDIS_HOST=$(echo $REDIS_URL | sed -n 's|redis://.*@\([^:]*\):.*|\1|p' || echo "fks_data_redis")
    if [ "$REDIS_HOST" = "$REDIS_URL" ]; then
        # Try without @
        REDIS_HOST=$(echo $REDIS_URL | sed -n 's|redis://\([^:/]*\):.*|\1|p' || echo "fks_data_redis")
        if [ "$REDIS_HOST" = "$REDIS_URL" ] || [ -z "$REDIS_HOST" ]; then
            REDIS_HOST="fks_data_redis"
        fi
    fi
    REDIS_PORT=$(echo $REDIS_URL | sed -n 's|redis://.*:\([0-9]*\)/.*|\1|p' || echo "6379")
    
    # Simple TCP connection check with timeout (max 10 attempts = 20 seconds)
    MAX_ATTEMPTS=10
    ATTEMPT=0
    while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
        if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$REDIS_HOST/$REDIS_PORT" 2>/dev/null; then
            echo "Redis is ready!"
            break
        fi
        echo "Redis not ready, waiting... ($((ATTEMPT+1))/$MAX_ATTEMPTS)"
        sleep 2
        ATTEMPT=$((ATTEMPT+1))
    done
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "Warning: Redis not available after $MAX_ATTEMPTS attempts. Continuing without Redis cache."
    fi
fi

# If a command is passed, execute it (for Celery worker/beat)
if [ "$#" -gt 0 ]; then
    echo "Executing command: $@"
    exec "$@"
fi

# Run the service (FastAPI with uvicorn)
# Migrated from Flask to FastAPI for better performance and async support
if command -v uvicorn > /dev/null 2>&1; then
    exec uvicorn src.main_fastapi:app \
        --host "${HOST}" \
        --port "${SERVICE_PORT}" \
        --workers 2 \
        --log-level info \
        --access-log \
        --no-use-colors
else
    # Fallback to Python module execution
    exec python -m uvicorn src.main_fastapi:app \
        --host "${HOST}" \
        --port "${SERVICE_PORT}" \
        --log-level info
fi
