"""
FKS Data Service - FastAPI Main Application

Migrated from Flask to FastAPI for better performance, async support, and type safety.
This is the main entry point for the FastAPI version of fks_data.
"""

import os
import logging
import sys
from datetime import datetime, UTC
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CollectorRegistry, Gauge, generate_latest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add src to path for local imports
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

# Initialize FastAPI app
app = FastAPI(
    title="FKS Data Service",
    description="Market data collection, validation, storage, and serving",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Service information
SERVICE_INFO = {
    "name": "fks_data",
    "version": "2.0.0",
    "description": "FKS Data Service - Market data collection and storage",
    "status": "healthy",
    "started_at": datetime.now(UTC).isoformat(),
    "framework": "FastAPI"
}

# Include routers
try:
    from src.api.routes.health import router as health_router
    app.include_router(health_router, tags=["health"])
    logger.info("✅ Health routes loaded")
except ImportError as e:
    logger.warning(f"⚠️ Health routes not available: {e}")

try:
    from src.api.routes.data import router as data_router
    app.include_router(data_router, tags=["data"])
    logger.info("✅ Data routes loaded")
except ImportError as e:
    logger.warning(f"⚠️ Data routes not available: {e}")

try:
    from src.api.routes.webhooks import router as webhooks_router
    app.include_router(webhooks_router, tags=["webhooks"])
    logger.info("✅ Webhook routes loaded")
except ImportError as e:
    logger.warning(f"⚠️ Webhook routes not available: {e}")

try:
    from src.api.routes.massive_futures import router as futures_router
    app.include_router(futures_router, tags=["futures"])
    logger.info("✅ Massive.com Futures routes loaded")
except ImportError as e:
    logger.warning(f"⚠️ Futures routes not available: {e}")

try:
    from src.api.routes.websocket import router as websocket_router
    app.include_router(websocket_router, tags=["websocket"])
    logger.info("✅ WebSocket routes loaded")
except ImportError as e:
    logger.warning(f"⚠️ WebSocket routes not available: {e}")

# Root endpoint
@app.get("/", response_class=JSONResponse)
async def root():
    """Root endpoint"""
    return {
        "service": "fks_data",
        "message": "FKS Data Service is running",
        "version": "2.0.0",
        "framework": "FastAPI",
        "health_check": "/health",
        "api_docs": "/docs"
    }

@app.get("/info", response_class=JSONResponse)
async def info():
    """Service information endpoint"""
    return SERVICE_INFO

@app.get("/status", response_class=JSONResponse)
async def status():
    """Detailed status endpoint"""
    return {
        **SERVICE_INFO,
        "uptime": str(datetime.now(UTC) - datetime.fromisoformat(SERVICE_INFO["started_at"].replace('Z', '+00:00'))),
        "endpoints": {
            "health": "/health",
            "info": "/info",
            "status": "/status",
            "data": "/api/v1/data",
            "futures": "/api/v1/futures",
            "docs": "/docs"
        }
    }

# -----------------------------------------------------------------------------
# Prometheus metrics - minimal exporter
# -----------------------------------------------------------------------------
registry = CollectorRegistry()
build_info = Gauge(
    "fks_build_info",
    "Build information",
    ["service", "version"],
    registry=registry,
)
build_info.labels(service="fks_data", version=SERVICE_INFO["version"]).set(1)

@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    return PlainTextResponse(generate_latest(registry).decode("utf-8"))

if __name__ == "__main__":
    import uvicorn
    host = os.getenv('FKS_DATA_HOST', '0.0.0.0')
    port = int(os.getenv('FKS_DATA_PORT', '8003'))
    logger.info(f"🚀 Starting FKS Data Service (FastAPI) on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
