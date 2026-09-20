"""Integration tests: FastAPI endpoints → feature generation → DuckDB logging.

The real feature-generation code runs against an isolated temp DB.
Only the MLflow model load and the XGBoost model itself are mocked so
tests remain fast and environment-independent.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from fastapi.testclient import TestClient

import src.serving.api as api_module
from src.serving.api import app

# A timestamp within the 400-hour seeded window (hour 350 of 400)
TARGET_TS = "2026-01-15T14:00:00"


def _mock_model() -> MagicMock:
    """Return a minimal model stub whose .predict() returns a fixed value."""
    m = MagicMock()
    m.predict.return_value = np.array([42000.0])
    return m


@pytest.fixture
def api_client(tmp_duckdb, seed_demand):
    """TestClient with a mock model and 400 hours of demand history in the temp DB."""
    with patch("src.serving.api.load_model"):
        with TestClient(app) as client:
            api_module._model = _mock_model()
            api_module._model_rmse = None  # no RMSE available → fallback CI
            yield client
    api_module._model = None
    api_module._model_rmse = None


@pytest.fixture
def api_client_no_data(tmp_duckdb):
    """TestClient with a mock model but an empty DB (no demand history)."""
    with patch("src.serving.api.load_model"):
        with TestClient(app) as client:
            api_module._model = _mock_model()
            api_module._model_rmse = None
            yield client
    api_module._model = None
    api_module._model_rmse = None


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health_returns_ok(api_client):
    resp = api_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["model_loaded"] is True


# ---------------------------------------------------------------------------
# /predict — happy path
# ---------------------------------------------------------------------------

def test_predict_returns_valid_structure(api_client):
    resp = api_client.post("/predict", json={"timestamp": TARGET_TS, "region": "CISO"})
    assert resp.status_code == 200
    body = resp.json()
    assert {"timestamp", "region", "demand_mwh", "confidence_interval"} <= body.keys()


def test_predict_demand_is_positive(api_client):
    resp = api_client.post("/predict", json={"timestamp": TARGET_TS, "region": "CISO"})
    assert resp.status_code == 200
    assert resp.json()["demand_mwh"] > 0


def test_predict_confidence_interval_is_ordered(api_client):
    resp = api_client.post("/predict", json={"timestamp": TARGET_TS, "region": "CISO"})
    assert resp.status_code == 200
    lo, hi = resp.json()["confidence_interval"]
    assert lo < hi


def test_predict_logs_to_duckdb(api_client):
    """A successful /predict call must persist the prediction to the predictions table."""
    from src.data.store import load_predictions

    api_client.post("/predict", json={"timestamp": TARGET_TS, "region": "CISO"})

    preds = load_predictions(region="CISO")
    assert len(preds) == 1
    assert float(preds.iloc[0]["demand_mwh"]) == pytest.approx(42000.0, abs=1.0)


# ---------------------------------------------------------------------------
# /predict — error paths
# ---------------------------------------------------------------------------

def test_predict_insufficient_history_returns_400(api_client_no_data):
    """POST /predict with no historical data in DB must return HTTP 400."""
    resp = api_client_no_data.post(
        "/predict", json={"timestamp": TARGET_TS, "region": "CISO"}
    )
    assert resp.status_code == 400
    assert "Insufficient historical data" in resp.json()["detail"]


def test_predict_model_not_loaded_returns_503(tmp_duckdb):
    """POST /predict when no model is loaded must return HTTP 503."""
    with patch("src.serving.api.load_model"):
        with TestClient(app) as client:
            api_module._model = None  # no model
            resp = client.post("/predict", json={"timestamp": TARGET_TS, "region": "CISO"})
    api_module._model = None

    assert resp.status_code == 503
