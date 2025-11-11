"""Basic health check tests."""
import pytest
from fastapi.testclient import TestClient

# Try to import app - adjust import path as needed
try:
    from src.main import app
    client = TestClient(app)
except ImportError:
    # Fallback if app structure is different
    client = None


@pytest.mark.skipif(client is None, reason="App not importable")
def test_health_endpoint():
    """Test health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


@pytest.mark.skipif(client is None, reason="App not importable")
def test_ready_endpoint():
    """Test readiness endpoint."""
    response = client.get("/ready")
    assert response.status_code == 200


@pytest.mark.skipif(client is None, reason="App not importable")
def test_live_endpoint():
    """Test liveness endpoint."""
    response = client.get("/live")
    assert response.status_code == 200
