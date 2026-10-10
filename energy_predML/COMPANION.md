# Companion Guide — Energy Grid Load Forecasting Pipeline

This document is a walkthrough of the repository's architecture, stack, and source code. Its purpose is to help a reader understand **what each piece does, why it exists, and how it fits into the end-to-end ML pipeline**.

---

## Table of Contents

1. [Pipeline at a Glance](#pipeline-at-a-glance)
2. [Stack Overview](#stack-overview)
3. [Project Layout](#project-layout)
4. [Data Layer — Ingestion & Storage](#data-layer)
5. [Feature Engineering](#feature-engineering)
6. [Model Training & Experiment Tracking](#model-training)
7. [Model Serving](#model-serving)
8. [Monitoring & Drift Detection](#monitoring)
9. [Orchestration](#orchestration)
10. [User Interface](#user-interface)
11. [Testing Strategy](#testing-strategy)
12. [Containerization & Deployment](#containerization)
13. [CI/CD](#cicd)
14. [Key Design Decisions](#key-design-decisions)

---

## 1. Pipeline at a Glance <a name="pipeline-at-a-glance"></a>

The system forecasts hourly electricity demand (in MWh) for a US grid region. Data flows through five stages:

```
EIA API  →  Ingest & Store  →  Feature Engineering  →  Model Training  →  Serving
                                                              ↑                │
                                                              │                ▼
                                                         Retrain         Predictions
                                                              ↑                │
                                                              │                ▼
                                                        Drift Detection  ←  Monitoring
```

Each stage is independently runnable (locally or in a container), and the monitoring loop can trigger retraining automatically when the model's input distribution shifts.

---

## 2. Stack Overview <a name="stack-overview"></a>

| Component | Tool | Role in the Pipeline |
|-----------|------|----------------------|
| **Data source** | EIA Open Data API | Provides free, hourly US electricity demand data by grid region. The API serves as the single external data dependency. |
| **Storage** | DuckDB + Parquet | DuckDB is an embedded analytical database (no server process). It stores raw demand rows and logged predictions. Parquet files hold versioned feature sets for reproducible training. Together they form a lightweight feature store. |
| **Feature engineering** | pandas | All feature transforms (temporal, lag, rolling) are pure pandas operations. No framework overhead — just vectorised DataFrame manipulations. |
| **ML framework** | XGBoost | Gradient-boosted trees, the dominant algorithm for tabular regression. Handles missing values natively, trains fast, and integrates well with SHAP for explainability. |
| **Hyperparameter tuning** | Optuna | Bayesian optimisation library that samples the hyperparameter space efficiently. Integrates directly with MLflow so every trial is logged. |
| **Experiment tracking** | MLflow | Logs parameters, metrics, and model artifacts for every training run. The Model Registry holds versioned model binaries so the serving layer always loads the latest registered model. |
| **Orchestration** | Prefect | Defines data pipelines as Python functions decorated with `@flow` / `@task`. Lighter than Airflow: no DAG files, no scheduler daemon required for local runs. Tasks get automatic retry and caching. |
| **Serving** | FastAPI | Async Python web framework with automatic OpenAPI docs. The `/predict` endpoint generates features on-the-fly, runs inference, and logs every prediction for downstream monitoring. |
| **Monitoring** | Evidently AI | Statistical drift detection library. Compares feature distributions between the training era and recent data. When drift exceeds a configurable threshold, it signals the need for retraining. |
| **Dashboard** | Streamlit | Rapid-prototyping UI framework. Four tabs give a visual overview of forecasts, historical accuracy, feature importance, and drift reports. |
| **Containers** | Docker + Compose | Each pipeline stage has its own Dockerfile with only the dependencies it needs. `docker-compose.yml` wires them together with shared volumes and networking. |
| **CI** | GitHub Actions | Runs all 49 tests on every push and PR. The workflow is a single job on `ubuntu-latest` with Python 3.12. |

---

## 3. Project Layout <a name="project-layout"></a>

```
energy_predML/
├── src/                         # All application source code
│   ├── config.py                # Central configuration (env vars, paths)
│   ├── data/                    # Ingestion: fetch, validate, store
│   ├── features/                # Feature engineering: temporal, lag, pipeline
│   ├── models/                  # Training: baseline, xgboost, evaluation
│   ├── serving/                 # API: FastAPI app + prediction logic
│   ├── monitoring/              # Drift detection: Evidently reports
│   └── workflows/               # Prefect flows: ingest, train, monitor
├── streamlit_app/app.py         # Dashboard UI
├── tests/                       # 49 tests (unit + integration)
├── docker/                      # Per-service Dockerfiles & requirements
├── data/                        # Runtime data (gitignored)
│   ├── raw/                     # Raw JSON from EIA API
│   ├── processed/               # Versioned Parquet feature files
│   ├── predictions/             # (reserved for future use)
│   ├── drift_reports/           # Evidently HTML + JSON reports
│   └── energy.duckdb            # DuckDB database file
├── docker-compose.yml           # Multi-service orchestration
├── Makefile                     # Convenience targets (local-* and docker-*)
├── requirements.txt             # Full Python dependencies
└── .github/workflows/test.yml   # CI pipeline
```

The `src/` package mirrors the pipeline stages: **data → features → models → serving → monitoring**, with **workflows** tying them together.

---

## 4. Data Layer — Ingestion & Storage <a name="data-layer"></a>

### `src/data/fetch.py` — EIA API Client

Fetches hourly electricity demand for a given grid region (default: CISO — California ISO) from the US Energy Information Administration's REST API.

- **Pagination**: The EIA API returns at most 5,000 rows per request. `fetch_demand()` loops, incrementing the `offset` parameter, until all records are retrieved.
- **Raw archival**: Every API response is saved as a timestamped JSON file under `data/raw/`. This provides an immutable audit trail — if validation or storage logic changes, the raw data can be reprocessed.
- **Defaults**: If no date range is specified, it fetches the last 30 days.

### `src/data/validate.py` — Schema Validation

Sits between fetch and store as a quality gate.

- Checks that every record contains the required fields (`period`, `respondent`, `type`, `value`).
- Renames columns to the internal schema (`timestamp`, `region`, `demand_mwh`).
- Drops rows with null demand values and sorts by timestamp.
- Raises `ValueError` early if the data is malformed, preventing bad data from entering DuckDB.

### `src/data/store.py` — DuckDB Interface

DuckDB is an embedded columnar database (similar to SQLite, but optimised for analytics). It requires no server — just a file on disk.

- **`demand` table**: Stores `(timestamp, region, demand_mwh)` with a composite primary key on `(timestamp, region)`. This makes ingestion idempotent: re-running the pipeline for the same date range inserts zero duplicate rows.
- **`predictions` table**: Stores `(timestamp, region, demand_mwh, predicted_at)`. Every API prediction is logged here so that monitoring can later compare predictions against actuals.
- **Upsert logic**: `upsert_demand()` performs an anti-join — it only inserts rows whose `(timestamp, region)` pair does not already exist. This avoids the complexity of SQL `MERGE` statements.
- **`get_connection()`**: Returns a DuckDB connection, creating the database file and tables on first run. A `read_only` mode is available for serving (concurrent reads without locking).

### Why this design?

The three-file separation (fetch → validate → store) follows the Extract-Validate-Load pattern. Each step is independently testable, and the raw JSON archive means no data is ever lost even if downstream logic changes. DuckDB was chosen over PostgreSQL to keep the project self-contained — no database server to install or manage.

---

## 5. Feature Engineering <a name="feature-engineering"></a>

### `src/features/temporal.py` — Time-Based Features

Extracts **11 features** from the timestamp column:

| Feature | Type | Purpose |
|---------|------|---------|
| `hour_of_day` | Integer 0–23 | Captures intra-day demand patterns (peak at noon/evening, trough at night) |
| `day_of_week` | Integer 0–6 | Weekday vs weekend patterns |
| `day_of_month` | Integer 1–31 | Monthly billing cycle effects |
| `month` | Integer 1–12 | Seasonal heating/cooling demand |
| `week_of_year` | Integer 1–52 | Finer seasonal granularity |
| `is_weekend` | Binary 0/1 | Commercial load drops on weekends |
| `is_holiday` | Binary 0/1 | US federal holidays (hardcoded set) |
| `hour_sin`, `hour_cos` | Float | Cyclical encoding of hour — ensures the model knows hour 23 is close to hour 0 |
| `dow_sin`, `dow_cos` | Float | Cyclical encoding of day-of-week |

Cyclical encoding uses sine/cosine transforms (`sin(2π × value / period)`) so that tree-based models can learn the continuity of time. Without this, XGBoost would treat hour 23 and hour 0 as maximally distant.

### `src/features/lag.py` — Lagged & Rolling Features

Extracts **10 features** from historical demand values:

| Feature | Window | Purpose |
|---------|--------|---------|
| `demand_lag_1h` | 1 hour | Short-term momentum |
| `demand_lag_24h` | 24 hours | Same-hour-yesterday (strong daily pattern) |
| `demand_lag_48h` | 48 hours | Two-days-ago reference |
| `demand_lag_168h` | 168 hours | Same-hour-last-week (weekly seasonality) |
| `demand_roll_24h_mean/std` | 24-hour rolling | Recent daily trend and volatility |
| `demand_roll_7d_mean/std` | 7-day rolling | Weekly trend and volatility |
| `demand_diff_1h` | 1 hour | Hourly rate of change |
| `demand_diff_24h` | 24 hours | Daily rate of change |

**Leakage prevention** is critical here. Every rolling window uses `.shift(1)` before `.rolling()`, ensuring the current row's actual demand value never leaks into its own features. This is the single most important design constraint in time-series feature engineering.

The first ~168 rows (7 days) of any dataset will have NaN lag features — the pipeline drops these rows rather than imputing, which would introduce artificial signal.

### `src/features/pipeline.py` — Feature Orchestration

Ties temporal and lag features together into a single function:

1. Load raw demand from DuckDB for a given region and date range.
2. Apply `add_temporal_features()`.
3. Apply `add_lag_features()`.
4. Drop NaN rows from the lag warm-up period.
5. Validate that no nulls remain.
6. Return a fully-featured DataFrame.

The result can be saved to a versioned Parquet file (`features_v{timestamp}.parquet`). Versioning means training is reproducible — you can always trace which feature set produced which model.

`get_latest_features()` is a convenience function that loads the most recent Parquet file, used by the training workflow to avoid rebuilding features every run.

---

## 6. Model Training & Experiment Tracking <a name="model-training"></a>

### `src/models/baseline.py` — Naive Baseline

The simplest possible model: **predict demand = demand 24 hours ago** (the `demand_lag_24h` feature). This establishes a performance floor (RMSE ~1,927 MWh) that any ML model must beat to justify its complexity. The baseline is logged to MLflow so it appears alongside ML runs in the experiment dashboard.

### `src/models/xgboost_model.py` — XGBoost Training

Two modes of operation:

1. **`train_xgboost(train_df, val_df)`**: Trains a single XGBoost regressor with given (or default) hyperparameters. Uses `early_stopping_rounds=50` on the validation set to prevent overfitting — training stops when validation RMSE hasn't improved for 50 consecutive boosting rounds. Default configuration achieves RMSE ~564 MWh.

2. **`tune_xgboost(df, n_trials, n_splits)`**: Optuna-driven hyperparameter search. The search space covers learning rate, tree depth, regularisation strength, subsampling ratios, and ensemble size. Each trial uses `TimeSeriesSplit` with `n_splits` folds — this is a walk-forward validation where each fold trains on all prior data and validates on the next chunk. The objective minimises mean RMSE across folds. The best configuration (RMSE ~501 MWh, R² 0.95) is returned as a parameter dictionary.

### `src/models/evaluate.py` — Metrics

Computes four standard regression metrics:

- **RMSE** (root mean squared error): Primary metric. Penalises large errors more than small ones — appropriate for grid forecasting where a big miss is costly.
- **MAE** (mean absolute error): Robust to outliers. Gives the average magnitude of error.
- **R²** (coefficient of determination): Fraction of variance explained. 0.95 means the model captures 95% of demand variability.
- **MAPE** (mean absolute percentage error): Scale-independent metric. Excludes zero-demand hours to avoid division by zero.

### MLflow Integration

MLflow is the experiment tracking backbone. Every training run logs:

- **Parameters**: All hyperparameters (learning rate, max depth, etc.).
- **Metrics**: RMSE, MAE, R², MAPE on the validation set.
- **Artifacts**: The serialised XGBoost model binary.

The best model is registered to the **MLflow Model Registry** under a named model. The serving layer loads the latest registered version at startup — this decouples training from serving. A new model can be trained and registered without restarting the API; the next API restart picks it up automatically.

The training workflow also writes `data/train_cutoff.txt` — the maximum timestamp in the training data. This timestamp is used by the monitoring workflow to split data into "training era" (reference) and "post-training" (current) for drift detection.

---

## 7. Model Serving <a name="model-serving"></a>

### `src/serving/api.py` — FastAPI Application

Two endpoints:

- **`GET /health`**: Returns `{"status": "healthy", "model_loaded": true, "region": "CISO"}`. Used by Docker health checks and load balancers.
- **`POST /predict`**: Accepts `{"timestamp": "2025-06-15T14:00:00", "region": "CISO"}`, returns `{"demand_mwh": 32451.7, "confidence_interval": [31470.1, 33433.3]}`.

On startup (via FastAPI's lifespan context manager), the app:
1. Queries the MLflow Model Registry for the latest model version.
2. Falls back to the latest `xgboost_tuned` run if the registry is empty.
3. Retrieves the validation RMSE from the training run's logged metrics.
4. Holds the model and RMSE in module-level globals for the lifetime of the process.

### `src/serving/predict.py` — Inference Logic

Generating features at inference time is the most intricate part of the serving layer:

1. **Load history**: Fetches the last 336 hours (14 days) of demand data from DuckDB. This is enough to compute the longest lag feature (168h = 7 days) plus the longest rolling window (7 days), with margin.
2. **Build features**: Applies the same temporal and lag feature functions used during training. This ensures feature consistency — the model sees identically computed inputs at training and inference time.
3. **Handle the target row**: If the requested timestamp doesn't exist in DuckDB yet (which is the normal case for future predictions), a placeholder row is appended. Its demand value is irrelevant — it's overwritten by the model's prediction.
4. **Predict**: Calls `model.predict()` on the single feature row.
5. **Confidence interval**: If the training run's validation RMSE is available, the interval is `prediction ± 1.96 × RMSE` (95% Gaussian coverage). Otherwise, falls back to `± 4%`.

Every prediction is logged to the `predictions` table in DuckDB via `log_prediction()`. This creates the data trail needed for monitoring to compare forecasts against actuals.

---

## 8. Monitoring & Drift Detection <a name="monitoring"></a>

### `src/monitoring/drift.py` — Evidently Reports

Evidently AI provides statistical tests to detect distribution shifts.

- **`generate_drift_report(reference_df, current_df, feature_columns)`**: Runs a two-sample statistical test (e.g., Kolmogorov-Smirnov for numerical features) on each feature column between the reference (training-era) and current (post-training) data. Returns:
  - `drifted_columns`: Which features have shifted significantly (p < 0.05).
  - `drift_share`: Fraction of features that drifted (0.0 to 1.0).
  - `dataset_drift`: Boolean flag (true if `drift_share > 0.3`).

- **`generate_regression_report(reference_df, current_df)`**: Compares prediction error distributions between reference and current periods. Requires both `target` (actual) and `prediction` columns — only generated when enough matched predictions exist (≥ 48 hours).

Reports are saved as both HTML (human-readable, embeddable in Streamlit) and JSON (machine-parseable).

### `src/workflows/monitor.py` — Monitoring Workflow

The monitoring flow ties drift detection into the retraining loop:

1. Build features for all available data.
2. Split into reference (data before `train_cutoff.txt`) and current (data after).
3. Run the drift report on feature distributions.
4. If matched predictions exist, run the regression report.
5. **Retraining trigger**: If `drift_share > DRIFT_THRESHOLD` (configurable, default 0.3), write a `data/retrain_needed` flag file and attempt to call `run_training()` directly.

The threshold is intentionally configurable via the `DRIFT_THRESHOLD` environment variable. A lower threshold (e.g., 0.1) triggers retraining more aggressively; a higher one (e.g., 0.5) is more conservative. The right value depends on business tolerance for model staleness versus retraining cost.

---

## 9. Orchestration <a name="orchestration"></a>

### Prefect Flows (`src/workflows/`)

Prefect is used to define the three pipeline stages as Python flows:

**`ingest.py`** — Data ingestion:
```
@flow ingest_flow(region, start, end)
  ├── @task fetch_data()      # retries=3, retry_delay=60s, cached
  ├── @task save_raw()
  ├── @task validate_data()
  └── @task store_data()
```
The `@task` decorator gives each step independent retry logic and result caching. If the EIA API is temporarily unavailable, `fetch_data` retries three times with a 60-second backoff. Task-level caching means re-running the flow with the same inputs skips the API call entirely.

**`train.py`** — Model training:
```
run_training(use_tuning, n_trials, rebuild_features)
  ├── Check MLflow connectivity
  ├── Load or rebuild features
  ├── Temporal 80/20 split
  ├── Train baseline → log to MLflow
  ├── Train XGBoost default → log to MLflow
  ├── (optional) Optuna tuning → log best to MLflow + register
  └── Write train_cutoff.txt
```

**`monitor.py`** — Drift monitoring:
```
run_monitoring(train_ratio, region)
  ├── Build features
  ├── Split reference / current
  ├── Generate drift report
  ├── (optional) Generate regression report
  └── If drift > threshold → flag + retrain
```

Each workflow is runnable as a standalone script (`python -m src.workflows.ingest`) or via Docker Compose. Prefect's UI is not used in this project — the flows are executed directly as Python functions, with Prefect providing the task-level retry/caching/logging infrastructure.

---

## 10. User Interface <a name="user-interface"></a>

### `streamlit_app/app.py` — Dashboard

A four-tab Streamlit application:

**Tab 1 — Forecast**: Shows the last 24 hours of actual demand overlaid with model predictions and a 95% confidence band. Displays summary metrics (RMSE, MAE, average demand).

**Tab 2 — Historical Accuracy**: Uses the last 30% of available data as a validation set. Plots actual vs. predicted time series and a residual chart. Displays RMSE, MAE, MAPE, and R².

**Tab 3 — Feature Importance**: Extracts `feature_importances_` from the loaded XGBoost model and renders a horizontal bar chart sorted by importance. This shows which features (e.g., `demand_lag_24h`, `hour_of_day`) the model relies on most.

**Tab 4 — Drift Reports**: Scans `data/drift_reports/` for HTML files generated by Evidently, lets the user select one, and renders it in an iframe.

The sidebar provides a region selector (CISO, ERCO, PJM, MISO) and data source information. The model is loaded once using `@st.cache_resource` to avoid reloading on every user interaction.

---

## 11. Testing Strategy <a name="testing-strategy"></a>

The test suite comprises **49 tests** split across unit and integration levels.

### Test Isolation

`tests/conftest.py` provides two key fixtures:

- **`tmp_duckdb`**: Monkeypatches the `DB_PATH` constant to a temporary file. Every test gets its own empty database, preventing cross-test contamination and protecting the production database.
- **`seed_demand`**: Inserts 400 hours of synthetic sinusoidal demand data (starting 2026-01-01) into the temporary database. This provides enough history for lag features (168h minimum) without requiring real API data.

### Unit Tests (25)

| Module | What's tested |
|--------|--------------|
| `test_fetch.py` | API pagination logic, empty response handling (HTTP mocked) |
| `test_validate.py` | Schema validation, null handling, timestamp sorting |
| `test_store.py` | Upsert idempotency, filtered loads, prediction logging |
| `test_temporal.py` | Feature value ranges, weekend detection, cyclical encoding bounds, DataFrame immutability |
| `test_lag.py` | Lag value correctness, NaN warm-up period, leakage prevention |
| `test_evaluate.py` | Metric computation for perfect, known-error, and edge cases |

### Integration Tests (24)

| Module | What's tested |
|--------|--------------|
| `test_pipeline_integration.py` (data) | Full fetch→validate→store→load roundtrip, idempotency, region filtering |
| `test_pipeline_integration.py` (features) | End-to-end `build_features()`, no-null guarantee, leakage verification |
| `test_api.py` | Health endpoint, prediction endpoint, confidence interval ordering, DB logging, error responses (400, 503) |
| `test_monitor_integration.py` | No-drift scenario, drift-triggered flag, regression report skipping, stale-cutoff early return |

### CI

GitHub Actions runs the full suite on every push and PR to `main`. The workflow uses Python 3.12 with pip caching for fast installs.

---

## 12. Containerization & Deployment <a name="containerization"></a>

### Per-Service Dockerfiles

Rather than a single monolithic image, each pipeline stage has its own Dockerfile under `docker/`:

| Image | Base | Key dependencies | Purpose |
|-------|------|-------------------|---------|
| `Dockerfile.ingest` | python:3.12-slim | requests, prefect, duckdb | Data ingestion |
| `Dockerfile.train` | python:3.12-slim | xgboost, optuna, mlflow | Model training |
| `Dockerfile.serve` | python:3.12-slim | fastapi, xgboost, streamlit, mlflow | API + dashboard |
| `Dockerfile.monitor` | python:3.12-slim | evidently, duckdb | Drift monitoring |

This keeps images small — the monitoring container doesn't carry XGBoost, and the ingestion container doesn't carry MLflow.

### `docker-compose.yml`

Defines six services:

- **`mlflow`** (always-on): MLflow tracking server with `--serve-artifacts`. Uses a Docker volume for persistence. Health-checked via HTTP.
- **`api`** (always-on): FastAPI serving on port 8000. Depends on MLflow being healthy. Shares `./data/` via bind mount.
- **`streamlit`** (always-on): Dashboard on port 8501. Same image as `api` (shares the serve Dockerfile). Mounts `./streamlit_app/` for hot reload during development.
- **`ingest`** (one-shot, `tools` profile): Runs the ingestion flow and exits.
- **`train`** (one-shot, `tools` profile): Runs the training flow and exits. Depends on MLflow.
- **`monitor`** (one-shot, `tools` profile): Runs the monitoring flow and exits.

The one-shot services use Docker Compose profiles so they don't start with `docker compose up` — they're triggered explicitly via `docker compose run --rm ingest`.

### Makefile

Provides two parallel sets of targets:

- **`local-*`** targets: Run everything in a local virtualenv. Useful during development.
- **`docker-*`** targets: Run everything via Docker Compose. Used for production-like execution and demos.

Key targets: `make setup` (install deps), `make local-fetch` / `make docker-fetch` (ingest data), `make local-train` / `make docker-train` (train model), `make local-serve` / `make docker-serve` (start API + dashboard), `make test` (run tests), `make clean` (remove all generated data).

---

## 13. CI/CD <a name="cicd"></a>

`.github/workflows/test.yml` defines a single GitHub Actions workflow:

- **Trigger**: Every push to `main` and every pull request targeting `main`.
- **Environment**: Ubuntu latest, Python 3.12, pip caching.
- **Steps**: Checkout → Install dependencies → `pytest tests/ -v --tb=short`.
- **Coverage**: All 49 tests (unit + integration). No external services required — DuckDB is embedded and all HTTP calls are mocked or use test fixtures.

---

## 14. Key Design Decisions <a name="key-design-decisions"></a>

### No data leakage

Every lag and rolling feature uses `.shift()` before computation. This is the most critical invariant in the codebase. The integration tests explicitly verify it: they check that `demand_lag_24h` at row `t` equals `demand_mwh` at row `t-24`, not at row `t` itself.

### Idempotent ingestion

The DuckDB upsert performs an anti-join rather than an `INSERT OR REPLACE`. Re-running the ingestion pipeline for the same date range inserts zero rows. This makes the pipeline safe to run on overlapping schedules without data duplication.

### Temporal splits everywhere

Training uses an 80/20 temporal split (never shuffled). Optuna tuning uses `TimeSeriesSplit` for walk-forward cross-validation. The monitoring workflow splits on the training cutoff timestamp. Shuffling time-series data would let the model "see the future" during training, producing unrealistically optimistic metrics.

### Closed monitoring loop

Every API prediction is logged to DuckDB with a `predicted_at` timestamp. The monitoring workflow inner-joins predictions with actuals on `(timestamp, region)`. This creates a real feedback loop: the system can compare what it predicted against what actually happened, which is the foundation for regression performance monitoring.

### Configurable drift threshold

The retraining trigger (`DRIFT_THRESHOLD`) is an environment variable, not a hardcoded constant. This allows operators to tune the sensitivity without changing source code — set it lower for aggressive retraining, higher for conservative.

### RMSE-based confidence intervals

The API returns `prediction ± 1.96 × validation_RMSE`, providing a 95% Gaussian confidence band calibrated to the model's actual validation error. This is more meaningful than a fixed percentage band because it reflects real model uncertainty. The RMSE is fetched from the MLflow run's logged metrics at model load time.

### Dual execution modes

The same source code runs both locally (via virtualenv + `make local-*`) and in containers (via `docker compose` + `make docker-*`). The `./data/` directory is shared between both modes, so a model trained locally can be served in Docker and vice versa.
