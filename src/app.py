"""
FKS Data Service - Standalone Flask Application

This is a simplified version that doesn't depend on the framework template.
"""

import os
import sys
from flask import Flask, jsonify, request
import logging
from datetime import datetime, timezone

# Add src to path for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Service information
SERVICE_INFO = {
    "name": "fks_data",
    "version": "1.0.0",
    "description": "FKS Data Service - Market data collection and storage",
    "status": "healthy",
    "started_at": datetime.utcnow().isoformat()
}

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "fks_data",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0"
    })

@app.route('/info', methods=['GET'])
def info():
    """Service information endpoint"""
    return jsonify(SERVICE_INFO)

@app.route('/status', methods=['GET'])
def status():
    """Detailed status endpoint"""
    return jsonify({
        **SERVICE_INFO,
        "uptime": str(datetime.utcnow() - datetime.fromisoformat(SERVICE_INFO["started_at"].replace('Z', '+00:00'))),
        "endpoints": {
            "health": "/health",
            "info": "/info",
            "status": "/status",
            "data": "/api/v1/data"
        }
    })

@app.route('/api/v1/data', methods=['GET'])
def get_data():
    """Sample data endpoint"""
    return jsonify({
        "message": "FKS Data Service is operational",
        "data": {
            "timestamp": datetime.utcnow().isoformat(),
            "sample_data": "Market data would be here",
            "status": "ready"
        }
    })

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "fks_data",
        "message": "FKS Data Service is running",
        "health_check": "/health",
        "api_docs": "/info"
    })

if __name__ == '__main__':
    logger.info("🚀 Starting FKS Data Service")
    logger.info(f"📊 Service: {SERVICE_INFO['name']} v{SERVICE_INFO['version']}")
    
    # Get configuration from environment
    host = os.getenv('FKS_DATA_HOST', '0.0.0.0')
    port = int(os.getenv('FKS_DATA_PORT', '4200'))
    debug = os.getenv('FKS_DEBUG', 'false').lower() == 'true'
    
    logger.info(f"🌐 Starting server on {host}:{port}")
    
    try:
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True
        )
    except Exception as e:
        logger.error(f"❌ Failed to start server: {e}")
        sys.exit(1)
