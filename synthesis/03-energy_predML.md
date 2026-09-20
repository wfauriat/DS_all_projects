# energy_predML — technical synthesis

> A full **MLOps lifecycle** on a deliberately well-understood problem: hourly electricity-demand
> forecasting (MWh) for a US grid region, from live EIA API data through feature engineering,
> experiment-tracked training, REST serving, drift monitoring with auto-retraining, and a dashboard.
> The domain is intentionally boring so that the *lifecycle* is the subject.

---

## 1. Identity

| Item | Value |
|---|---|
| Domain | time-series regression: next-hour / next-day electricity demand for a grid region (default `CISO`) |
| Shape | linear MLOps pipeline, layered `src/` package, two interchangeable run modes (venv / Docker) |
| Stack | Python 3.11+, pandas, DuckDB, XGBoost, Optuna, scikit-learn, MLflow, Evidently AI, Prefect, FastAPI, Streamlit, Docker Compose |
| Size | ~60 files, ~4.8k lines incl. docs; `src/` ≈ 1.1k lines, tests ≈ 700 lines |
| Docs | `CLAUDE.md` (the original project brief + implementation checklist + status), `README.md`, `COMPANION.md` (a written walkthrough), `STRATEGY.md` (a focused RFC on the MLflow storage overhaul) |
| Measured results | baseline (lag-24h) RMSE 1927 → XGBoost default 564 → Optuna-tuned **501 MWh, R² 0.95, MAPE 1.35 %** |
| Tests | 25 unit + 24 integration = 49, all runnable with no services up |

---

## 2. Topology

```
EIA Open Data API  (/v2/electricity/rto/region-data/data/)
   │  fetch_demand()  paginated, 5000 rows/request
   ▼
data/raw/{REGION}_{ts}.json          (raw response, timestamped)
   │  validate_records()             schema check → typed DataFrame
   ▼
DuckDB  data/energy.duckdb
   ├─ demand(timestamp, region, demand_mwh)          PK (timestamp, region)
   └─ predictions(timestamp, region, demand_mwh, predicted_at)  PK (timestamp, region)
   │  build_features()  temporal (11) + lag/rolling/diff (10)
   ▼
data/processed/features_v{ts}.parquet   (versioned feature snapshots)
   │  run_training()
   ▼
MLflow  (tracking + Model Registry "energy_demand_xgboost")
   │  baseline run · xgboost_default run · Optuna study · xgboost_tuned run (registered)
   │  + data/train_cutoff.txt  = max(demand.timestamp) at training time
   ▼
FastAPI  GET /health · POST /predict         ← loads max(version) from the registry at startup
   │  features built on the fly from DuckDB history; every prediction logged back to DuckDB
   ▼
Monitoring  (Evidently)  reference = data before the cutoff, current = data after
   │  DataDriftPreset + RegressionPreset (on predictions ⋈ actuals)
   │  drift_share > DRIFT_THRESHOLD → touch data/retrain_needed → run_training(use_tuning=False)
   ▼
Streamlit dashboard  (Forecast · Historical Accuracy · Feature Importance · Drift Reports)
```

This is the only one of the four projects whose loop **closes back on itself**: predictions become
data, data becomes a drift verdict, and the verdict retrains the model that makes the predictions.

---

## 3. Entry points

| Entry point | Local target | Docker target |
|---|---|---|
| `python -m src.workflows.ingest` | `make local-fetch` | `make docker-fetch` (`compose run --rm ingest`) |
| `python -m src.workflows.train` | `make local-train` | `make docker-train` |
| `uvicorn src.serving.api:app` | `make local-serve` | `make docker-serve` (api + streamlit + mlflow) |
| `python -m src.workflows.monitor` | `make local-monitor` | `make docker-monitor` |
| `streamlit run streamlit_app/app.py` | `make local-dashboard` | in `docker-serve` |
| `mlflow server …` | `make local-mlflow` (idempotent: curls `/health` first) | `mlflow` compose service |
| `python -m src.features.pipeline` | module `__main__` block | — |
| `scripts/compare_models.py` | analysis script | — |

---

## 4. Module map (`src/`)

### `config.py`
Path constants derived from `PROJECT_ROOT` (`DATA_DIR`, `RAW_DATA_DIR`, `PROCESSED_DATA_DIR`,
`RETRAIN_FLAG_PATH`, `TRAIN_CUTOFF_PATH`) + a `pydantic-settings` `Settings`
(`EIA_API_KEY`, `MLFLOW_TRACKING_URI`, `DEFAULT_REGION`, `DRIFT_THRESHOLD`). Calls `setup_logging()`
at import — every module then uses `logging.getLogger(__name__)` with `%s`-style lazy formatting.

### `data/`
| Module | Content |
|---|---|
| `fetch.py` | EIA client. Offset pagination loop driven by `response.total`, `MAX_ROWS_PER_REQUEST = 5000`, ascending sort pushed into the API (`sort[0][column]=period`), 30 s timeout, defaults to "last 30 days". `save_raw_response()` writes a timestamped JSON of the raw payload |
| `validate.py` | `validate_records()` — required-column check → `to_datetime(utc=True)` → `to_numeric(errors="coerce")` → drop null demand (logged) → project to `(timestamp, region, demand_mwh)` sorted. Raises `ValueError` on empty or malformed input |
| `store.py` | the DuckDB layer. `get_connection(read_only)` creates both tables idempotently and degrades `read_only` to read-write when the file doesn't exist yet. `upsert_demand()` registers the DataFrame as a view and does `INSERT … WHERE NOT EXISTS`, returning the delta of `COUNT(*)` as "rows inserted". `log_prediction()` uses `ON CONFLICT … DO UPDATE` so only the latest prediction per `(timestamp, region)` survives. `load_demand`/`load_predictions` build `WHERE 1=1` + parameterised filters |

### `features/`
- `temporal.py` — 11 columns: `hour_of_day`, `day_of_week`, `day_of_month`, `month`, `week_of_year`,
  `is_weekend`, `is_holiday` (a static set of US federal holiday date strings), and **cyclical
  encodings** `hour_sin/cos`, `dow_sin/cos` (`sin/cos(2π·x/period)` — so hour 23 and hour 0 are
  adjacent in feature space, which a raw integer cannot express).
- `lag.py` — 10 columns: lags 1 h / 24 h / 48 h / 168 h; rolling mean+std over 24 h and 168 h; diffs
  over 1 h and 24 h. **Every rolling and diff feature is `.shift(1)` before the window**, so the
  current row's own target never enters its features. This is the single most important line-level
  discipline in the project and it is tested explicitly.
- `pipeline.py` — `NON_FEATURE_COLS = {"timestamp", "region", "demand_mwh"}` is the one definition of
  "what is a feature"; `build_features()` = load → sort → temporal → lag → `dropna()` (the lag warm-up)
  → assert zero nulls; `save_features()` writes `features_v{UTC ts}.parquet`; `get_latest_features()`
  takes the lexicographically last file (timestamp naming makes sort order = chronological order).

### `models/`
- `baseline.py` — the naive model is a *column*: `y_pred = df["demand_lag_24h"]`. Logged to MLflow like
  any other run. Having a cheap, honest baseline is what makes "RMSE 501" meaningful.
- `evaluate.py` — `compute_metrics()` → `{rmse, mae, r2, mape}`, with MAPE guarded against zero actuals.
- `xgboost_model.py` — `train_xgboost()` (explicit `eval_set` + `early_stopping_rounds=50`) and
  `tune_xgboost()` (Optuna `create_study(direction="minimize")` over 8 hyperparameters, objective =
  mean RMSE across **`TimeSeriesSplit`** folds — never `KFold`).

### `serving/`
- `predict.py` — the interesting part of inference: **features are generated at request time** from
  DuckDB history. `generate_features_for_timestamp()` loads `HISTORY_HOURS = 336` (14 days) of demand,
  requires ≥ 169 rows, appends a NaN placeholder row for the target hour if it isn't in the data yet,
  re-runs the *same* `add_temporal_features` / `add_lag_features` functions used in training (no
  train/serve skew), extracts the target row, and fills residual NaNs with 0 with a warning.
  `predict_demand()` returns the point estimate plus `± 1.96 × val_RMSE` (Gaussian 95 %), falling back
  to `± 4 %` when the RMSE is unknown.
- `api.py` — FastAPI with a `lifespan` that calls `load_model()`. **Dynamic model resolution**:
  `MlflowClient().search_model_versions("name='energy_demand_xgboost'")` → `max(version)`, so a retrain
  automatically promotes itself with no config change; the run's logged `rmse` metric is fetched and
  cached in a module global for the confidence interval; a fallback path searches the latest run tagged
  `model_type = 'xgboost_tuned'`. Error mapping: 503 (no model) / 400 (`ValueError` = insufficient
  history) / 500 (anything else). Prediction logging is wrapped so a logging failure never fails the
  request.

### `monitoring/drift.py`
Evidently 0.7-style API (`Report(metrics=[DataDriftPreset()]).run(...)` → a *snapshot*).
`generate_drift_report()` parses `snapshot.dump_dict()["metric_results"]`, counts columns whose
"Value drift for X" p-value < 0.05, and derives `drift_share = n_drifted / n_features`.
`generate_regression_report()` needs an explicit `DataDefinition(regression=[Regression(target=…, prediction=…)])`
and `Dataset.from_pandas`. `save_report()` writes both HTML and JSON, timestamped.

### `workflows/`
- `ingest.py` — the only place Prefect appears: four `@task`s (with `retries=3, retry_delay_seconds=60,
  cache_key_fn=task_input_hash` on the network call) composed by one `@flow`. The tasks are thin
  wrappers over the plain functions, so the business logic is Prefect-free and testable.
- `train.py` — `run_training(use_tuning, n_trials, rebuild_features)`: a **pre-flight probe** of the
  MLflow server with a helpful error message; `set_experiment("energy_grid_forecast")`; features from
  Parquet or rebuilt; an **80/20 temporal split by index** (`df.iloc[:split]`, never shuffled); then
  baseline → default XGBoost → Optuna → retrain with best params → `mlflow.xgboost.log_model(...,
  registered_model_name=...)`. Finally writes `data/train_cutoff.txt` with `max(demand.timestamp)`
  **read from DuckDB, not from the Parquet snapshot**, so the cutoff reflects everything available.
- `monitor.py` — `run_monitoring()`:
  1. `build_features()` over all data;
  2. **cutoff-based split**: reference = rows ≤ cutoff (what the model saw), current = rows after;
     early-returns a no-drift result when fewer than 24 new rows exist (so monitoring right after a
     retrain correctly reports nothing); falls back to 80/20 when no cutoff file exists;
  3. data-drift report;
  4. regression report from `_load_matched_predictions()` — an **inner join of the `predictions` table
     with actual `demand`** on `(timestamp, region)`, requiring ≥ 48 matched rows;
  5. if `drift_share > settings.DRIFT_THRESHOLD`: `RETRAIN_FLAG_PATH.touch()`, then a **lazy import**
     of `run_training` inside a `try/except ImportError` — present in local mode, absent in the slim
     monitor container, where the flag file is the hand-off to `make docker-monitor`'s shell logic.
     On a training exception the flag is intentionally left in place so the next run retries.

### `streamlit_app/app.py`
Four tabs (Forecast with a 24 h recursive-style forward view, Historical Accuracy, Feature Importance
from `model.feature_importances_`, Drift Reports rendering the saved Evidently HTML). `@st.cache_resource`
for the model; the same registry-then-run fallback as the API (duplicated rather than shared — a small
DRY leak).

### `scripts/compare_models.py`
An honest evaluation experiment rather than a pipeline component: **1-step-ahead vs 24 h recursive
forecast**. The recursive function writes each prediction back into `demand_mwh` and into the
downstream `demand_lag_*` cells before predicting the next step, so the horizon is forecast using only
predicted or genuinely past values. This is the standard way to expose that a 1-step RMSE of ~500 MWh
does not imply a 24 h-ahead RMSE of ~500 MWh.

---

## 5. Two run modes, one codebase

The dual-mode design is explicit and documented, including its sharp edges:

| | local (`local-*`) | docker (`docker-*`) |
|---|---|---|
| Python | `.venv` on the host | four per-service images |
| MLflow store | `./mlflow_data/` on the host | Docker **named volume** `mlflow_data` |
| `./data/` | host directory | bind-mounted into every container |
| Retraining on drift | in-process `run_training(use_tuning=False)` | container exits; the Makefile checks `data/retrain_needed` and invokes `docker-train`, clearing the flag only on success |

The two modes deliberately **do not share trained models**; the README says to stay in one mode per
workflow. The named volume for Docker mode exists so the MLflow server can run as root without
permission conflicts.

**Per-service images** (`docker/Dockerfile.{ingest,train,monitor,serve}` +
`requirements-{ingest,train,monitor,serve}.txt`): the serving image excludes optuna/sklearn/prefect/
evidently; the monitoring image excludes all ML packages. This is what makes the `ImportError` branch
in `monitor.py` a *designed* control-flow path rather than an accident.

Compose details: `profiles: ["tools"]` keeps the one-shot `ingest`/`train`/`monitor` services out of
`docker compose up`; `user: "${UID}:${GID}"` (exported by the Makefile) keeps `./data` owned by the
developer; `depends_on: mlflow: condition: service_healthy` with a Python-based healthcheck; the train
container deliberately does **not** mount the MLflow volume, because artifacts go through the HTTP
proxy.

**`STRATEGY.md`** is worth reading on its own as a model of a small infrastructure RFC: it states the
problem (filesystem tracking backend bakes absolute artifact paths into metadata, is deprecated, and
writes artifacts client-side), the target architecture (SQLite backend + `--serve-artifacts` so URIs
become `mlflow-artifacts://` and all I/O is proxied), why it works, and a stepwise implementation plan.

---

## 6. Testing strategy

Two clearly separated layers, both runnable with **no services running**.

- **Isolation primitive**: `tmp_duckdb` monkeypatches `src.data.store.DB_PATH` to a `tmp_path` file, so
  no test can ever touch `data/energy.duckdb`. `seed_demand` then inserts 400 hours of seeded
  sinusoidal demand (`default_rng(42)`), which is enough history for the 168 h lag and 7 d rolling
  windows.
- **Unit (25)**: mocked EIA HTTP responses incl. pagination; validation (schema, nulls, sorting);
  temporal features (ranges, weekend detection, cyclical bounds in [-1, 1]); lag features (exact index
  arithmetic — `result.iloc[30]["demand_lag_24h"] == df.iloc[6]["demand_mwh"]` — NaN warm-up, and an
  explicit **no-leakage** assertion); metrics (perfect predictions, known errors, MAPE).
- **Integration (24)**: real components wired together against the temp DB —
  fetch→validate→store→load roundtrip and idempotency; `build_features` end-to-end with a no-null
  guarantee; the FastAPI app via `TestClient` with only the *model* mocked (so real feature generation
  runs), asserting response shape, that the prediction landed in the `predictions` table, 400 on
  missing history, 503 on missing model; and the monitoring workflow with `generate_drift_report`
  mocked to controlled drift shares, asserting flag written / not written / early return on a stale cutoff.
- CI: `.github/workflows/test.yml` runs the whole suite on push and PR to `main` (Python 3.12, pip cache).

The mocking boundary is chosen thoughtfully each time: mock the *network* and the *model*, never the
data layer or the feature code.

---

## 7. Idioms worth extracting

1. **Leakage prevention as a coding convention**: every rolling/diff feature is `shift(1)`-ed, and a
   unit test encodes the invariant. In time series this is the difference between a real model and a
   meaningless one.
2. **Cyclical encoding** for periodic integers (hour, day-of-week) instead of raw ordinals or one-hot.
3. **A baseline that is one column.** `demand_lag_24h` as the naive model, logged to MLflow with the
   same metrics as the real model, is the cheapest possible honesty mechanism.
4. **Temporal splits everywhere**: `iloc[:split]` for train/val, `TimeSeriesSplit` for CV; never a
   shuffle, never `KFold`.
5. **Train/serve skew eliminated by construction**: the serving path imports the *same*
   `add_temporal_features`/`add_lag_features` functions, and `NON_FEATURE_COLS` is the single
   definition of the feature set used by training, serving, monitoring and the dashboard.
6. **The registry as the deployment mechanism.** The API resolves `max(version)` at startup, so
   "deploying a model" is "registering a model". The associated run's `rmse` metric is pulled along
   with it and drives the confidence interval — model *and* its uncertainty travel together.
7. **A closed monitoring loop needs a predictions table.** Logging every served prediction, then
   inner-joining with actuals once they arrive, is what turns "drift monitoring" from an input-distribution
   heuristic into real measured error. The `≥ 48 matched rows` guard prevents reports on noise.
8. **Cutoff-based drift reference.** `data/train_cutoff.txt` makes "reference vs current" mean exactly
   "what the model was trained on vs what arrived since", which is the only definition under which
   "monitor immediately after retraining ⇒ no drift" is true.
9. **A flag file as an inter-container control channel.** `data/retrain_needed` + `ImportError` on the
   training import lets one workflow function behave correctly in both a fat local environment and a
   slim container, with the Makefile closing the loop in the Docker case.
10. **Versioned artefacts by timestamped filename** (`features_v{ts}.parquet`), with sort order
    doubling as recency order.
11. **Idempotent ingestion** via `INSERT … WHERE NOT EXISTS` and a natural primary key — re-running a
    fetch over an overlapping window is always safe.
12. **A pre-flight dependency probe with an actionable error** (the MLflow reachability check that
    prints the exact `make` target to run).

## 8. Known gaps / deliberate deferrals

- **No scheduler is actually running.** Prefect is used as a decorator library; the daily-6 AM /
  weekly schedules described in the brief are listed as the top "high value, low effort" next step.
  The pipeline *can* run autonomously; it does not yet *do* so. (Contrast with `mediaElection27`'s
  EventBridge cron and `DE_pipeline_stream`'s Airflow.)
- Confidence intervals assume homoskedastic Gaussian error; quantile regression (`reg:quantileerror`)
  or conformal prediction is the documented upgrade.
- Only temporal features + autoregressive lags — **no weather**, which the docs identify as the single
  biggest expected accuracy gain (expected RMSE < 400).
- The 24 h recursive horizon is only explored in `scripts/compare_models.py`; the served model is
  1-step-ahead and needs history up to the target hour.
- US holidays are a hard-coded string set covering 2025–2026.
- Model-loading logic is duplicated between `serving/api.py` and `streamlit_app/app.py`.
- No authentication on the API; no cloud deployment; alerting on drift deliberately deferred.
