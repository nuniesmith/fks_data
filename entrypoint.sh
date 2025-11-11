#!/bin/bash
# Entrypoint script for fks_data

set -e

# Default values
SERVICE_NAME=${SERVICE_NAME:-fks_data}
SERVICE_PORT=${SERVICE_PORT:-8003}
HOST=${HOST:-0.0.0.0}

echo "Starting ${SERVICE_NAME} on ${HOST}:${SERVICE_PORT}"

# Run the service (Flask with gunicorn for production, or flask run for dev)
# For Docker, use gunicorn with gevent workers for async support
if command -v gunicorn > /dev/null 2>&1; then
    exec gunicorn src.app:app \
        --bind "${HOST}:${SERVICE_PORT}" \
        --workers 2 \
        --worker-class gevent \
        --timeout 120 \
        --access-logfile - \
        --error-logfile -
else
    # Fallback to Flask dev server
    export FLASK_APP=src.app:app
    export FLASK_RUN_HOST="${HOST}"
    export FLASK_RUN_PORT="${SERVICE_PORT}"
    exec flask run --host="${HOST}" --port="${SERVICE_PORT}"
fi
