#!/usr/bin/env bash
# =============================================================================
# Test commands — local mode and Docker mode
# Copy-paste individual blocks; do NOT run this file end-to-end.
# =============================================================================

# ── Shared helper ─────────────────────────────────────────────────────────────
PREDICT_TS="2026-02-10T14:00:00"
REGION="CISO"

_check_predictions() {
    python3 -c "
import duckdb
c = duckdb.connect('data/energy.duckdb', read_only=True)
print(c.execute('SELECT COUNT(*) AS n, MAX(predicted_at) AS latest FROM predictions').fetchdf())
"
}

_check_model_versions() {
    curl -s http://localhost:5000/api/2.0/mlflow/registered-models/get \
        -G --data-urlencode "name=energy_demand_xgboost" \
      | python3 -m json.tool \
      | grep '"version"'
}


# =============================================================================
# LOCAL MODE
# Runs in the .venv on the host. MLflow must stay up for the whole session.
# =============================================================================

# ── Step 1: start MLflow (dedicated terminal, keep it running) ────────────────
make local-mlflow

# ── Step 2: ingest data (skip if data/energy.duckdb already populated) ────────
make local-fetch

# ── Step 3: train + register model ───────────────────────────────────────────
make local-train

# Check which version was registered
_check_model_versions

# ── Step 4: serve API ─────────────────────────────────────────────────────────
make local-serve                      # blocks — open a new terminal for step 5+

# ── Step 5: smoke-test the API ───────────────────────────────────────────────
curl -s http://localhost:8000/health | python3 -m json.tool

curl -s -X POST http://localhost:8000/predict \
    -H "Content-Type: application/json" \
    -d "{\"timestamp\": \"$PREDICT_TS\", \"region\": \"$REGION\"}" \
  | python3 -m json.tool

# Confirm prediction was persisted
_check_predictions

# ── Step 6: monitoring + auto-retrain ─────────────────────────────────────────
make local-monitor

# ── Step 7: dashboard (optional, separate terminal) ───────────────────────────
make local-dashboard

# ── Step 8: force drift to test auto-retrain path ─────────────────────────────
python3 -c "
import src.workflows.monitor as m
m.DRIFT_THRESHOLD = 0.0
result = m.run_monitoring()
print('retrain_triggered:', result['retrain_triggered'])
print('flag exists:', __import__('pathlib').Path('data/retrain_needed').exists())
"


# =============================================================================
# DOCKER MODE
# All services run in containers. Shares ./data/ with local mode.
# MLflow storage is a separate Docker-managed named volume.
# =============================================================================

# ── Step 1: ingest data ───────────────────────────────────────────────────────
make docker-fetch

# ── Step 2: train + register model ───────────────────────────────────────────
make docker-train

# Check which version was registered (starts MLflow briefly)
docker compose up -d --wait mlflow
_check_model_versions
docker compose stop mlflow

# ── Step 3: serve (MLflow + API + Streamlit) ──────────────────────────────────
make docker-serve                     # blocks — open a new terminal for step 4+

# ── Step 4: smoke-test the API ───────────────────────────────────────────────
curl -s http://localhost:8000/health | python3 -m json.tool

curl -s -X POST http://localhost:8000/predict \
    -H "Content-Type: application/json" \
    -d "{\"timestamp\": \"$PREDICT_TS\", \"region\": \"$REGION\"}" \
  | python3 -m json.tool

# Confirm prediction was persisted
_check_predictions

# ── Step 5: monitoring (auto-triggers docker-train on drift) ──────────────────
make docker-monitor

# ── Step 6: force drift to test auto-retrain path ─────────────────────────────
# Temporarily lower the threshold inside the monitor container
docker compose run --rm monitor python3 -c "
import src.workflows.monitor as m
m.DRIFT_THRESHOLD = 0.0
result = m.run_monitoring()
print('retrain_triggered:', result['retrain_triggered'])
"
# If retrain_triggered is False (Docker mode), check for the flag file:
ls -la data/retrain_needed 2>/dev/null && echo "flag present — run: make docker-train"


# =============================================================================
# TEARDOWN
# =============================================================================

# Stop all containers and remove volumes (resets Docker MLflow state)
docker compose down -v

# Full clean (removes data, mlflow_data, caches — destructive)
# make clean
