# Energy Grid Load Forecasting

[![Tests](https://github.com/wfauriat/energy_predML/actions/workflows/test.yml/badge.svg)](https://github.com/wfauriat/energy_predML/actions/workflows/test.yml)

A production-grade ML pipeline that forecasts hourly electricity demand (MWh) for US grid regions. The system fetches live data from the EIA API, engineers features, trains XGBoost models with hyperparameter tuning, serves predictions via REST API, and monitors for data drift.

## Architecture

```
EIA API  ──>  Data Ingestion  ──>  Feature Engineering  ──>  Model Training
                (Prefect)            (Pandas/DuckDB)        (XGBoost/Optuna/MLflow)
                    │                                              │
                    ▼                                              ▼
              DuckDB Storage  <───── prediction logging ─── FastAPI Serving
              (demand table)                                 (POST /predict)
              (predictions table)                                  │
                    │                                              │
                    ▼                                              │
            Drift Monitoring  <────────────────────────────────────┘
              (Evidently AI)
              auto-retrains on drift
                    │
                    ▼
          Streamlit Dashboard
```

## Tech Stack

| Component | Tool |
|-----------|------|
| Data Source | EIA Open Data API |
| Storage | DuckDB + Parquet |
| Feature Engineering | Pandas |
| Model Training | XGBoost + Optuna |
| Experiment Tracking | MLflow |
| Serving | FastAPI |
| Monitoring | Evidently AI |
| Dashboard | Streamlit |
| Orchestration | Prefect |
| Containerization | Docker Compose |

## Quick Start

### Prerequisites

- Python 3.11+
- Docker (required for Docker mode; optional for local mode)
- Free EIA API key from https://www.eia.gov/opendata/register.php

### Setup

```bash
make setup
# Edit .env with your EIA API key
```

### Local Mode — everything runs in the venv, no Docker needed for individual steps

MLflow must be running before training or serving. Start it in a dedicated terminal, then run the rest of the pipeline in another.

```bash
# terminal 1 — keep this running throughout
make local-mlflow

# terminal 2
make local-fetch      # fetch data from EIA API
make local-train      # train + register model in MLflow
make local-serve      # FastAPI on :8000
make local-dashboard  # Streamlit on :8501 (optional third terminal)
make local-monitor    # drift detection
```

### Docker Mode — everything runs in containers

```bash
make docker-fetch     # ingest data (docker compose run)
make docker-train     # train + register model (docker compose run)
make docker-serve     # start MLflow + API + Streamlit (docker compose up)
make docker-monitor   # drift detection (docker compose run)
```

Services (Docker mode):
- **MLflow UI** — `http://localhost:5000`
- **FastAPI** — `http://localhost:8000` (docs at `/docs`)
- **Streamlit** — `http://localhost:8501`

Raw data and features (`./data/`) are stored on the host as a bind mount and shared between both modes.

MLflow storage differs per mode:
- **Local mode** writes to `./mlflow_data/` on the host.
- **Docker mode** uses a Docker-managed named volume (`mlflow_data`), which keeps the MLflow server running as root without permission issues and avoids the `mkdir -p` workaround.

The two modes do not share trained models — run `local-*` or `docker-*` targets consistently within a workflow.

Drift reports (`./data/drift_reports/`) are written to the shared `./data/` bind mount, so they are visible in the Streamlit dashboard regardless of which mode generated them.

## Project Structure

```
energy_predML/
├── src/
│   ├── config.py                 # Pydantic settings, path constants; DRIFT_THRESHOLD configurable via env
│   ├── data/
│   │   ├── fetch.py              # EIA API client with pagination
│   │   ├── validate.py           # Schema validation
│   │   └── store.py              # DuckDB storage: demand table + predictions table (logged per /predict call)
│   ├── features/
│   │   ├── temporal.py           # Hour, day, month, weekend, holiday, cyclical
│   │   ├── lag.py                # Lags (1h/24h/48h/168h), rolling, diffs
│   │   └── pipeline.py           # Feature orchestration + versioned Parquet
│   ├── models/
│   │   ├── baseline.py           # Naive baseline (lag_24h)
│   │   ├── xgboost_model.py      # XGBoost training + Optuna tuning
│   │   └── evaluate.py           # RMSE, MAE, R2, MAPE metrics
│   ├── serving/
│   │   ├── api.py                # FastAPI app (GET /health, POST /predict)
│   │   └── predict.py            # Feature generation + inference logic
│   ├── monitoring/
│   │   └── drift.py              # Evidently data drift + regression reports
│   └── workflows/
│       ├── ingest.py             # Prefect flow: fetch -> validate -> store
│       ├── train.py              # Training pipeline: baseline + XGBoost + Optuna + MLflow registry; writes data/train_cutoff.txt
│       └── monitor.py            # Drift detection + auto-retraining on drift threshold breach
├── streamlit_app/
│   └── app.py                    # Dashboard: forecast, accuracy, features, drift
├── scripts/
│   └── compare_models.py         # 1-step vs 24h recursive forecast comparison
├── tests/
│   ├── conftest.py               # Shared fixtures: tmp_duckdb, seed_demand
│   ├── test_data/                # EIA client, validation, pipeline integration
│   ├── test_features/            # Temporal, lag, pipeline integration
│   ├── test_models/              # Evaluation metrics
│   ├── test_serving/             # FastAPI endpoint integration
│   └── test_monitoring/          # Drift detection + retrain flag integration
├── docker/
│   ├── Dockerfile.ingest         # Data ingestion image (prefect + data stack only)
│   ├── Dockerfile.train          # Model training image (xgboost + optuna + mlflow)
│   ├── Dockerfile.monitor        # Drift monitoring image (evidently + data stack)
│   ├── Dockerfile.serve          # API + Streamlit image (xgboost inference + mlflow)
│   ├── requirements-ingest.txt
│   ├── requirements-train.txt
│   ├── requirements-monitor.txt
│   └── requirements-serve.txt
├── docker-compose.yml
├── Makefile
├── requirements.txt              # Full deps for local venv (make setup / make test)
└── .env.example
```

## API

### Health Check

```bash
curl http://localhost:8000/health
```

```json
{"status": "ok", "model_loaded": true, "region": "CISO"}
```

### Predict

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"timestamp": "2026-02-15T14:00:00", "region": "CISO"}'
```

```json
{
  "timestamp": "2026-02-15T14:00:00",
  "region": "CISO",
  "demand_mwh": 19574.3,
  "confidence_interval": [18791.3, 20357.2]
}
```

Interactive API docs available at `http://localhost:8000/docs`.

## Features

### Temporal Features (11)
- `hour_of_day`, `day_of_week`, `day_of_month`, `month`, `week_of_year`
- `is_weekend`, `is_holiday` (US federal holidays)
- Cyclical encoding: `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`

### Lag & Rolling Features (10)
- Lags: 1h, 24h, 48h, 168h (1 week)
- Rolling: 24h mean/std, 7-day mean/std (shifted to prevent leakage)
- Differences: 1h and 24h rate of change (shifted to prevent leakage)

## Model Performance

Evaluated on temporal validation split (last 20% of data):

| Model | RMSE (MWh) | MAE (MWh) | R2 | MAPE |
|-------|-----------|----------|------|------|
| Baseline (lag_24h) | 1927 | 1382 | 0.31 | 5.83% |
| XGBoost (default) | 564 | 360 | 0.94 | 1.51% |
| XGBoost (tuned) | 501 | 324 | 0.95 | 1.35% |

## Testing

```bash
make test
```

Runs 49 tests across two layers:

**Unit tests (25)** — single functions in isolation with dependencies mocked:
- EIA API client (mocked HTTP responses, pagination)
- Data validation (schema, null handling, sorting)
- Temporal features (ranges, weekend detection, cyclical bounds)
- Lag features (correctness, NaN warm-up, no data leakage)
- Evaluation metrics (perfect predictions, known errors, MAPE)

**Integration tests (24)** — multiple real components wired together against an isolated temp DuckDB; no running services required:
- Data pipeline: fetch → validate → store → load roundtrip, idempotency, region filtering, prediction log/overwrite
- Feature pipeline: `build_features` end-to-end, no-null guarantee after warm-up, lag-24h leakage check
- API (`/health`, `/predict`): response shape, prediction logged to DB, 400 on missing history, 503 on missing model
- Monitoring: no-drift/no-flag, drift → flag written, regression report skipped with <48 predictions, early return on stale cutoff

To run only the integration tests:

```bash
.venv/bin/python -m pytest tests/test_data/test_pipeline_integration.py \
                           tests/test_features/test_pipeline_integration.py \
                           tests/test_serving/ \
                           tests/test_monitoring/ -v
```

## Key Design Decisions

- **Temporal splits only** — never shuffle time-series data. Train on past, validate on future.
- **No data leakage** — all lag/diff features use `shift()` to ensure only past data is used.
- **Idempotent ingestion** — DuckDB upserts skip existing rows, safe to re-run.
- **Two explicit run modes** — `local-*` targets run everything in the venv; `docker-*` targets run everything in containers. Both modes share `./data/` (raw data, features, drift reports). MLflow storage is separate: local mode writes to `./mlflow_data/` on the host; Docker mode uses a named Docker volume. Run `local-*` or `docker-*` targets consistently within a workflow. MLflow uses `--serve-artifacts` so artifact URIs are proxy-relative, never tied to an absolute path.
- **Per-service Docker images** — each service installs only the dependencies it needs (`docker/Dockerfile.*` + `docker/requirements-*.txt`). The serving image excludes training packages (optuna, scikit-learn, prefect, evidently); the monitoring image excludes all ML packages.
- **Versioned features** — Parquet files are timestamped for reproducibility.
- **Closed monitoring loop** — every `/predict` response is logged to a `predictions` table in DuckDB. The monitoring workflow inner-joins this table with actual demand to compute real prediction error, replacing a crude lag-feature proxy. Drift reports are only generated once ≥48 matched rows accumulate.
- **Configurable drift threshold** — the fraction of drifted features that triggers retraining is set via `DRIFT_THRESHOLD` in `.env` (default `0.3`). When the share exceeds the threshold, the monitor writes `data/retrain_needed` and attempts `run_training(use_tuning=False)` directly (local mode). In Docker mode the container exits cleanly and the Makefile reads the flag file to invoke `docker-train`, clearing it only on success.
- **Dynamic model loading** — the API always loads the highest registered version from the MLflow model registry (`search_model_versions` + `max(version)`), so retraining automatically promotes the new model without any config change. At startup the serving layer also fetches the model's logged `rmse` metric from its MLflow run and uses it to compute confidence intervals as `prediction ± 1.96 × RMSE` (~95% Gaussian coverage), falling back to ±4% of the prediction if the metric is unavailable.
- **Cutoff-based drift reference** — training writes the DuckDB max timestamp to `data/train_cutoff.txt`. The monitor uses this as the split point: reference = everything the model was trained on, current = data that arrived after training. Running the monitor immediately after a retrain correctly reports no drift (zero new rows); drift only fires again once a daily fetch brings in genuinely new data. Falls back to a fixed 80/20 split on the first run before any cutoff exists.
