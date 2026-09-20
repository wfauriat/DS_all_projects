# Energy Grid Load Forecasting — ML Pipeline Project

## Project Overview

Build a production-grade ML pipeline that forecasts hourly electricity consumption for a US grid region. The system fetches live data daily from the EIA API, trains models with experiment tracking, serves predictions via API, and monitors for data drift.

This is a well-understood problem (intentionally chosen for rapid implementation) that showcases the full MLOps lifecycle: data ingestion, feature engineering, model training, serving, and monitoring.

---

## Business Problem

Predict next-day hourly electricity demand (MWh) for a specific grid region to enable:
- Grid operators to balance supply and demand
- Energy traders to optimize bidding strategies
- Utilities to plan generation dispatch

The model needs to be retrained regularly as consumption patterns shift seasonally and as the grid composition changes (e.g., increasing renewable penetration).

---

## Tech Stack

| Component | Tool | Why |
|-----------|------|-----|
| **Language** | Python 3.11+ | Standard for ML |
| **Data Source** | EIA Open Data API | Free, hourly US grid data |
| **Orchestration** | Prefect | Lightweight workflow management |
| **Feature Store** | DuckDB + Parquet | Local, SQL-queryable, versioned |
| **Experiment Tracking** | MLflow | Industry standard, model registry |
| **Model Training** | XGBoost / LightGBM | Best performance for tabular time series |
| **Hyperparameter Tuning** | Optuna | Integrates with MLflow |
| **Serving** | FastAPI | Fast, async, modern Python API |
| **UI** | Streamlit | Rapid prototyping, good for demos |
| **Monitoring** | Evidently AI | Drift detection, performance tracking |
| **Containerization** | Docker + Docker Compose | Reproducible deployment |
| **Storage** | Local filesystem / MinIO (optional) | Keep it simple initially |

---

## Architecture Diagram (Conceptual)

```
┌─────────────────┐
│   EIA API       │  (hourly electricity demand data)
└────────┬────────┘
         │ Daily fetch
         ▼
┌─────────────────────────────────────────────────┐
│  DATA INGESTION PIPELINE (Prefect)              │
│  • Fetch new data from EIA API                  │
│  • Validate schema                              │
│  • Store raw data in DuckDB                     │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│  FEATURE ENGINEERING PIPELINE (Prefect)         │
│  • Temporal features (hour, day, week, month)   │
│  • Lag features (demand 24h ago, 168h ago)      │
│  • Rolling aggregates (7-day mean)              │
│  • Weather enrichment (optional: temp, humidity)│
│  • Holiday indicator                            │
│  • Save features to Parquet (versioned)         │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│  MODEL TRAINING (MLflow + Optuna)               │
│  • Load features from Parquet                   │
│  • Temporal train/val split (no shuffle!)       │
│  • Baseline: SARIMAX or Prophet                 │
│  • Main model: XGBoost with time-based CV       │
│  • Log experiments to MLflow                    │
│  • Hyperparameter tuning with Optuna            │
│  • Register best model to MLflow registry       │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│  MODEL SERVING (FastAPI)                        │
│  • Load model from MLflow registry              │
│  • /predict endpoint (input: timestamp)         │
│  • Returns: forecasted demand + confidence      │
│  • /health endpoint for monitoring              │
│  • Log all predictions to DB                    │
└────────┬────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│  MONITORING & DRIFT DETECTION (Evidently)       │
│  • Compare recent predictions vs actuals        │
│  • Detect feature drift (input distribution)    │
│  • Detect target drift (demand distribution)    │
│  • Generate drift reports (HTML + metrics)      │
│  • Trigger retraining if drift threshold hit    │
└─────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│  USER INTERFACE (Streamlit)                     │
│  • Interactive forecast visualization           │
│  • Show historical accuracy                     │
│  • Display feature importance (SHAP optional)   │
│  • Show drift reports                           │
└─────────────────────────────────────────────────┘
```

---

## Project Structure

```
energy-forecast/
├── README.md
├── CLAUDE.md                    # This file
├── docker-compose.yml           # Orchestrates all services
├── Makefile                     # Convenience commands
├── .env.example                 # Environment variables template
│
├── data/
│   ├── raw/                     # Raw EIA API responses (timestamped)
│   ├── processed/               # Feature-engineered Parquet files
│   └── predictions/             # Logged predictions
│
├── notebooks/
│   └── exploratory.ipynb        # EDA and prototyping
│
├── src/
│   ├── __init__.py
│   ├── config.py                # Configuration management
│   │
│   ├── data/
│   │   ├── __init__.py
│   │   ├── fetch.py             # EIA API client
│   │   ├── validate.py          # Schema validation
│   │   └── store.py             # DuckDB interface
│   │
│   ├── features/
│   │   ├── __init__.py
│   │   ├── temporal.py          # Time-based features
│   │   ├── lag.py               # Lagged demand features
│   │   └── pipeline.py          # Feature engineering orchestration
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── baseline.py          # SARIMAX / Prophet baseline
│   │   ├── xgboost_model.py     # XGBoost training + tuning
│   │   └── evaluate.py          # Metrics calculation
│   │
│   ├── serving/
│   │   ├── __init__.py
│   │   ├── api.py               # FastAPI app
│   │   └── predict.py           # Prediction logic
│   │
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── drift.py             # Evidently drift detection
│   │   └── alerts.py            # Alerting logic (optional)
│   │
│   └── workflows/
│       ├── __init__.py
│       ├── ingest.py            # Prefect flow: data ingestion
│       ├── train.py             # Prefect flow: model training
│       └── monitor.py           # Prefect flow: drift monitoring
│
├── streamlit_app/
│   └── app.py                   # Streamlit dashboard
│
├── mlflow/                      # MLflow artifact storage (gitignored)
├── prefect/                     # Prefect database (gitignored)
│
├── tests/
│   ├── test_data/
│   ├── test_features/
│   └── test_models/
│
├── requirements.txt             # Python dependencies
└── Dockerfile                   # Container definition
```

---

## Implementation Tasks (Sequential)

### Phase 0: Setup & Configuration
- [ ] Initialize git repository
- [ ] Create virtual environment (`python -m venv venv`)
- [ ] Install dependencies from `requirements.txt`
- [ ] Get free EIA API key from https://www.eia.gov/opendata/register.php
- [ ] Create `.env` file with `EIA_API_KEY=your_key_here`
- [ ] Set up `config.py` to load environment variables

### Phase 1: Data Ingestion (Day 1)
- [ ] **Task 1.1:** Implement EIA API client in `src/data/fetch.py`
  - Fetch hourly electricity demand data for a specific region (e.g., CAISO, ERCOT)
  - Use the `/v2/electricity/rto/region-data/data/` endpoint
  - Handle pagination and rate limiting
  - Save raw JSON responses to `data/raw/` with timestamps
  
- [ ] **Task 1.2:** Implement DuckDB storage layer in `src/data/store.py`
  - Create schema for raw demand data (timestamp, demand_mwh, region)
  - Insert fetched data into DuckDB table
  - Implement upsert logic (idempotent inserts)
  
- [ ] **Task 1.3:** Create Prefect flow in `src/workflows/ingest.py`
  - Orchestrate fetch → validate → store
  - Schedule to run daily at 6 AM UTC
  - Add retry logic for API failures

### Phase 2: Feature Engineering (Day 1-2)
- [ ] **Task 2.1:** Implement temporal features in `src/features/temporal.py`
  - Extract: hour_of_day, day_of_week, month, is_weekend, is_holiday
  - Use `pandas.DatetimeIndex` for efficient extraction
  
- [ ] **Task 2.2:** Implement lag features in `src/features/lag.py`
  - Create lags: demand_24h_ago, demand_168h_ago (1 week)
  - Create rolling means: demand_7day_mean, demand_30day_mean
  - Handle missing values at the start of the series
  
- [ ] **Task 2.3:** Create feature pipeline in `src/features/pipeline.py`
  - Load raw data from DuckDB
  - Apply all feature transformations
  - Save to versioned Parquet: `data/processed/features_v{timestamp}.parquet`
  - Implement feature validation (check for nulls, ranges)
  
- [ ] **Task 2.4:** Create Prefect flow for feature engineering
  - Trigger after data ingestion completes
  - Log feature statistics to MLflow

### Phase 3: Model Training (Day 2-3)
- [ ] **Task 3.1:** Set up MLflow tracking
  - Initialize MLflow experiment: `energy_grid_forecast`
  - Configure artifact storage location
  - Create helper functions for logging params/metrics
  
- [ ] **Task 3.2:** Implement baseline model in `src/models/baseline.py`
  - Use `statsmodels` SARIMAX or `prophet`
  - Train on 80% of data, validate on 20% (temporal split)
  - Log to MLflow: RMSE, MAE, R²
  
- [ ] **Task 3.3:** Implement XGBoost model in `src/models/xgboost_model.py`
  - Define time-series cross-validation (sklearn `TimeSeriesSplit`)
  - Train XGBoost regressor with default params
  - Compare against baseline
  
- [ ] **Task 3.4:** Integrate Optuna for hyperparameter tuning
  - Define search space: learning_rate, max_depth, n_estimators, etc.
  - Use 5-fold time-series CV for objective function
  - Log all trials to MLflow
  - Register best model to MLflow Model Registry
  
- [ ] **Task 3.5:** Create training Prefect flow in `src/workflows/train.py`
  - Load latest features
  - Train baseline + XGBoost
  - Log everything to MLflow
  - Promote best model to "Production" stage in registry

### Phase 4: Model Serving (Day 3-4)
- [ ] **Task 4.1:** Build FastAPI app in `src/serving/api.py`
  - `GET /health` — returns status
  - `POST /predict` — input: `{"timestamp": "2024-01-15T14:00:00"}`, output: `{"demand_mwh": 45000, "confidence_interval": [43000, 47000]}`
  - Load model from MLflow registry on startup
  - Add request validation with Pydantic
  
- [ ] **Task 4.2:** Implement prediction logic in `src/serving/predict.py`
  - Generate features for input timestamp
  - Call model.predict()
  - Return prediction + optional confidence bands
  - Log prediction to DuckDB (for monitoring)
  
- [ ] **Task 4.3:** Containerize API with Dockerfile
  - Base image: `python:3.11-slim`
  - Install dependencies
  - Expose port 8000
  - Entry point: `uvicorn src.serving.api:app --host 0.0.0.0`
  
- [ ] **Task 4.4:** Create docker-compose.yml
  - Service: `api` (FastAPI)
  - Service: `mlflow` (MLflow server)
  - Service: `prefect` (Prefect server, optional)
  - Shared volume for data/

### Phase 5: Monitoring & Drift Detection (Day 4-5)
- [ ] **Task 5.1:** Implement drift detection in `src/monitoring/drift.py`
  - Use Evidently `DataDriftPreset` and `RegressionPreset`
  - Compare reference data (training set) vs. current data (last 7 days of predictions)
  - Generate HTML report + JSON metrics
  
- [ ] **Task 5.2:** Create monitoring Prefect flow in `src/workflows/monitor.py`
  - Run weekly (or daily if you want)
  - Load predictions + actuals from last N days
  - Generate drift report
  - If drift detected → trigger retraining flow
  
- [ ] **Task 5.3:** Implement alerting (optional)
  - Send email/Slack notification if drift detected
  - Log alert to monitoring database

### Phase 6: User Interface (Day 5)
- [ ] **Task 6.1:** Build Streamlit dashboard in `streamlit_app/app.py`
  - **Tab 1: Forecast** — show next 24 hours prediction with confidence bands
  - **Tab 2: Historical Accuracy** — plot predicted vs actual over last 30 days, show RMSE/MAE
  - **Tab 3: Feature Importance** — bar chart of XGBoost feature importances
  - **Tab 4: Drift Reports** — display latest Evidently HTML report
  
- [ ] **Task 6.2:** Add Streamlit to docker-compose
  - Service: `streamlit`
  - Expose port 8501
  - Mount code directory for hot reload during dev

### Phase 7: Documentation & Polish (Day 6)
- [ ] **Task 7.1:** Write comprehensive README.md
  - Project description
  - Setup instructions (`make setup`, `make run`)
  - Architecture diagram
  - API documentation
  - Screenshots of Streamlit dashboard
  
- [ ] **Task 7.2:** Create Makefile with useful commands
  - `make setup` — create venv, install deps
  - `make fetch-data` — run data ingestion once
  - `make train` — trigger training flow
  - `make serve` — start FastAPI + Streamlit via docker-compose
  - `make test` — run pytest suite
  
- [ ] **Task 7.3:** Add tests
  - Unit tests for feature engineering
  - Integration test for API endpoint
  - Mock tests for EIA API client
  
- [ ] **Task 7.4:** Create `.gitignore`
  - Ignore: `venv/`, `data/`, `mlflow/`, `*.pyc`, `.env`

---

## Key Technical Decisions & Rationale

### Why DuckDB?
- Embedded SQL database, no server needed
- Fast analytical queries on time-series data
- Easy export to Parquet for feature store
- Low operational overhead for a portfolio project

### Why Prefect over Airflow?
- Lighter weight, easier to run locally
- Better Python-native API (no need to define DAGs in config)
- Good free tier for cloud orchestration if you want to deploy later
- Still industry-relevant (Airflow is also fine, just heavier)

### Why XGBoost?
- Best performance on tabular data (proven in countless Kaggle competitions)
- Handles missing values natively
- Fast training and inference
- Good feature importance via SHAP integration
- Time-series forecasting is still a regression problem at its core

### Why MLflow?
- Industry standard for experiment tracking
- Model registry with versioning and staging (dev/staging/production)
- Integrates with virtually every ML framework
- Can deploy models directly via `mlflow models serve` if needed

### Temporal Validation is Critical
- **Never shuffle time-series data** — always use temporal splits
- Train on `[t0, t1]`, validate on `[t1, t2]`, test on `[t2, t3]`
- Use `TimeSeriesSplit` from sklearn for cross-validation
- This prevents leakage and gives realistic performance estimates

---

## Data Source Details

### EIA API
- **Endpoint:** `https://api.eia.gov/v2/electricity/rto/region-data/data/`
- **Key parameters:**
  - `facets[respondent][]` — region code (e.g., `CISO` for California ISO, `ERCO` for ERCOT Texas)
  - `facets[type][]` — `D` for demand
  - `frequency` — `hourly`
  - `start` / `end` — date range
  - `api_key` — your free API key
- **Response format:** JSON with nested data array
- **Rate limit:** 5000 requests/hour (very generous)
- **Historical data:** Goes back several years, plenty for training

**Example request:**
```
GET https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key=YOUR_KEY&facets[respondent][]=CISO&facets[type][]=D&frequency=hourly&start=2023-01-01T00&end=2024-01-01T00
```

### Weather Data (Optional Enrichment)
If you want to add temperature as a feature (highly correlated with demand):
- **Open-Meteo API** — free, no key, historical + forecast
- Join weather data to demand data by timestamp + location
- Features: temperature, humidity, wind speed

---

## Success Criteria

By the end of this project, you should have:

1. ✅ A running data pipeline that fetches new data daily
2. ✅ Reproducible model training with experiment tracking
3. ✅ A deployed API that serves predictions
4. ✅ A monitoring system that detects drift and triggers retraining
5. ✅ A user-facing dashboard to visualize forecasts
6. ✅ Everything containerized and runnable via `docker-compose up`
7. ✅ Clean, well-documented code that a hiring manager can review

---

## Next Steps (After MVP)

### Quick Wins
- **Multi-region support** — Fetch and forecast for ERCO (Texas), PJM (East Coast), MISO (Midwest) alongside CISO. Most code already supports a `region` parameter.
- **Weather features** — Add temperature/humidity from Open-Meteo API (free, no key needed). Temperature is the #1 external driver of electricity demand. Join by timestamp + location.
- **SHAP explanations** — Add per-prediction feature importance via `shap.TreeExplainer` (works out of the box with XGBoost). Surface in API response and Streamlit dashboard.
- **Calibrated confidence intervals** — Replace fixed-width bands with quantile regression (XGBoost `quantile` objective) or conformal prediction for proper uncertainty quantification.

### Medium Effort
- **Scheduled orchestration** — Wire up Prefect schedules so ingestion runs daily at 6AM UTC and monitoring runs weekly, fully automated.
- **Automatic retraining trigger** — When drift is detected (already implemented), automatically kick off the training pipeline instead of just logging a warning.
- **Prediction logging** — Store every API prediction in DuckDB so actual vs predicted accuracy can be computed over time (needed for real production monitoring).
- **LightGBM comparison** — Add a LightGBM model alongside XGBoost. The infrastructure already supports it — just add `src/models/lightgbm_model.py` and log to MLflow.
- **API authentication** — Add API key auth or OAuth to the FastAPI endpoints.

### Larger Projects
- **Cloud deployment** — Deploy the API to AWS Lambda or GCP Cloud Run, use S3 for artifacts, hosted DB for storage.
- **Prometheus + Grafana** — Replace Evidently HTML reports with real-time metrics dashboards (prediction latency, error rates, drift scores).
- **Online/incremental learning** — Update the model incrementally as new data arrives instead of full retraining.
- **Multi-step probabilistic forecasting** — Predict full 24h distribution (not just point estimates) using DeepAR or Temporal Fusion Transformer.
- **Feature store migration** — Move from versioned Parquet files to Feast for more robust feature management and serving.
- **CI/CD pipeline** — GitHub Actions to run tests, lint, build Docker images, and auto-deploy on merge to main.

---

## Estimated Timeline

- **Day 1:** Data ingestion + feature engineering
- **Day 2:** Model training + MLflow integration
- **Day 3:** API serving + containerization
- **Day 4:** Monitoring + drift detection
- **Day 5:** Streamlit UI + polish
- **Day 6:** Documentation + testing

**Total:** ~6 days of focused work to a production-ready MVP.

---

## Common Pitfalls to Avoid

1. **Shuffling time-series data** — always use temporal splits
2. **Leaking future information** — ensure lag features only use past data
3. **Ignoring seasonality** — electricity demand has strong daily/weekly/seasonal patterns
4. **Over-engineering early** — get the MVP working first, then add complexity
5. **Not logging predictions** — you need prediction history for drift detection
6. **Forgetting to version features** — always timestamp your Parquet files

---

## Questions to Consider During Implementation

- How do you handle missing data from the EIA API (e.g., if a region doesn't report for a few hours)?
- What's your strategy for cold-start predictions (when you don't have 7 days of lag features yet)?
- How do you decide when drift is "significant enough" to retrain?
- What's the business cost of prediction error (overestimating vs underestimating demand)?

---

## References & Resources

- **EIA API Docs:** https://www.eia.gov/opendata/
- **MLflow Docs:** https://mlflow.org/docs/latest/index.html
- **Prefect Docs:** https://docs.prefect.io/
- **Evidently AI Docs:** https://docs.evidentlyai.com/
- **XGBoost Time Series:** https://xgboost.readthedocs.io/en/stable/tutorials/index.html
- **FastAPI Docs:** https://fastapi.tiangolo.com/

---

## Notes for Claude Code Session

When implementing this project:
1. Start with `src/config.py` and `requirements.txt`
2. Build incrementally — test each component before moving to the next
3. Use type hints throughout (`from typing import List, Dict, Optional`)
4. Add docstrings to all functions
5. Use `logging` instead of `print()` for better debugging
6. Write unit tests as you go (easier than retrofitting later)
7. Commit frequently with clear messages

The goal is a **working, production-flavored pipeline** that demonstrates MLOps competence, not a perfect solution. Ship the MVP, then iterate.

---

## Implementation Status

All MVP phases complete. One optional item (alerting) intentionally deferred.

### Phase 0 — Setup ✅
- [x] Git repository, virtual environment, dependencies
- [x] EIA API key, `.env`, `config.py` with Pydantic settings

### Phase 1 — Data Ingestion ✅
- [x] EIA API client with pagination and rate limiting (`src/data/fetch.py`)
- [x] DuckDB storage with idempotent upsert (`src/data/store.py`)
- [x] Prefect ingest flow: fetch → validate → store (`src/workflows/ingest.py`)

### Phase 2 — Feature Engineering ✅
- [x] 11 temporal features incl. cyclical encoding (`src/features/temporal.py`)
- [x] 10 lag/rolling/diff features, all shifted to prevent leakage (`src/features/lag.py`)
- [x] Feature pipeline with versioned Parquet output (`src/features/pipeline.py`)
- [x] Prefect feature flow wired to ingest flow

### Phase 3 — Model Training ✅
- [x] MLflow experiment `energy_grid_forecast`, SQLite backend, `--serve-artifacts`
- [x] Naive baseline: lag_24h (RMSE 1927 MWh)
- [x] XGBoost default (RMSE 564 MWh) and Optuna-tuned (RMSE 501 MWh, R² 0.95)
- [x] All runs logged; best model registered to MLflow Model Registry
- [x] Training cutoff written to `data/train_cutoff.txt` for drift reference

### Phase 4 — Model Serving ✅
- [x] FastAPI: `GET /health`, `POST /predict` with Pydantic validation
- [x] Feature generation at inference time from DuckDB history
- [x] Every prediction logged to `predictions` table in DuckDB
- [x] Confidence interval: `±1.96 × val RMSE` fetched from MLflow run at startup
- [x] 4 per-service Docker images; `docker-compose.yml` with MLflow container

### Phase 5 — Monitoring ✅
- [x] Evidently `DataDriftPreset` + `RegressionPreset` reports
- [x] Cutoff-based drift split (reference = training data, current = post-cutoff)
- [x] Auto-retraining: drift share > `DRIFT_THRESHOLD` (configurable via `.env`, default 0.3) writes flag and triggers `run_training(use_tuning=False)`
- [x] Closed monitoring loop: predictions joined with actuals via DuckDB inner join
- [ ] Alerting via email/Slack — deferred (optional)

### Phase 6 — User Interface ✅
- [x] Streamlit dashboard: Forecast, Historical Accuracy, Feature Importance, Drift Reports tabs
- [x] Streamlit service in `docker-compose.yml`

### Phase 7 — Documentation & Testing ✅
- [x] README: architecture, quick start, API docs, model performance, design decisions
- [x] Makefile: `local-*` and `docker-*` targets for all pipeline steps
- [x] 25 unit tests (data, features, models) + 24 integration tests (pipeline, API, monitoring)
- [x] `tests/conftest.py` with `tmp_duckdb` and `seed_demand` fixtures for DB isolation
- [x] `.gitignore`

---

## Post-MVP Progress & Prioritized Next Steps

### Done since MVP
- **RMSE-based confidence intervals** — replaced hardcoded ±2% with `±1.96 × val RMSE` from the model's MLflow run; falls back to ±4% when metric unavailable (`src/serving/predict.py`)
- **Configurable drift threshold** — `DRIFT_THRESHOLD` moved from hardcoded constant to `Settings` in `config.py`; set via `.env` without touching source (`src/config.py`, `src/workflows/monitor.py`)
- **Integration tests** — 24 tests covering data pipeline, feature pipeline, API endpoints, and monitoring workflow; `tmp_duckdb` fixture isolates every test from production DB
- **CI/CD (GitHub Actions)** — `.github/workflows/test.yml` runs all 49 tests on every push and PR to main (Python 3.12, pip cache); status badge on README; `pytest` and `httpx` added to `requirements.txt`

### Remaining — prioritized

#### High value, low effort
- **Scheduled orchestration** — Wire Prefect schedules so ingestion runs daily at 6AM UTC and monitoring runs weekly. Transforms the project from "pipeline that can run" to "pipeline that does run".

#### Meaningful model improvement
- **Weather features** — Temperature is the #1 external driver of electricity demand. Open-Meteo API is free, no key needed, has historical + forecast data. Join by timestamp + location; add `temperature_c`, `humidity`, `wind_speed`. Expected to push tuned RMSE below 400 MWh.
- **SHAP explanations** — `shap.TreeExplainer` works out of the box with XGBoost. Surface per-prediction feature contributions in the API response and Streamlit Feature Importance tab.
- **Quantile regression** — Current `±1.96 × RMSE` assumes constant variance. Train q=0.1 and q=0.9 XGBoost models (`objective='reg:quantileerror'`) for proper heteroskedastic intervals. Requires changes to `train.py`, `predict.py`, and the MLflow registry.
- **LightGBM comparison** — Add `src/models/lightgbm_model.py`, log both models in the same training run, let the registry always hold the winner. Infrastructure already supports it.

#### Infrastructure
- **API authentication** — API key header (`X-API-Key`) via FastAPI middleware. Simple to add, makes the project deployable without being an open endpoint.
- **Cloud deployment** — Push the API to AWS Lambda (via Mangum) or GCP Cloud Run, S3/GCS for MLflow artifacts. Transforms "runs locally" to "actually deployed".
- **Prometheus + Grafana** — Replace Evidently HTML reports with real-time dashboards: prediction latency, error rates, drift scores as time-series.
- **Multi-step probabilistic forecasting** — Predict full 24h distribution using DeepAR or Temporal Fusion Transformer instead of single-point estimates.
