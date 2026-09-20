# MLflow Infrastructure Overhaul Strategy

## Problem

The current setup uses MLflow's **filesystem tracking backend** (`--backend-store-uri /path/mlruns`).
This has three fundamental problems:

1. **Absolute paths baked into metadata** — when a model is trained, MLflow stores the
   artifact location as the absolute path on the machine where training ran. Any other
   machine (or container) must mount the `mlruns/` directory at the exact same absolute
   path, requiring the `${PWD}/mlruns` volume hack.

2. **Filesystem backend is deprecated** — MLflow 3.x warns that the filesystem backend
   will be removed. The recommended replacement is a database backend (SQLite or PostgreSQL).

3. **Artifact writes go through the client, not the server** — without `--serve-artifacts`,
   MLflow tells the client "write artifacts to /abs/path/mlruns/...". The client then tries
   to write directly to that path. This breaks when the client is a local process and the
   server is inside a container (the path doesn't exist on the host). The `--serve-artifacts`
   flag was intended to fix this, but in MLflow 3.x it only works correctly when paired with
   a database backend (not filesystem).

---

## Target Architecture

```
Training (local)          MLflow container           Storage (shared volume)
─────────────────         ─────────────────          ──────────────────────
mlflow.log_model()   →    HTTP API (port 5000)   →   /app/mlflow.db  (SQLite)
  uploads artifacts  →    --serve-artifacts       →   /app/artifacts/ (files)
  via HTTP POST            proxies all I/O
```

- **Backend store**: `sqlite:////app/mlflow.db` — metadata (runs, params, metrics, tags)
- **Artifact root**: `/app/artifacts` — model files, plots, etc.
- **Artifact transport**: HTTP proxy via `--serve-artifacts` — client never touches the filesystem directly
- **Volume**: single bind mount `./mlflow_data:/app` covers both db and artifacts

### Why this works

With a database backend + `--serve-artifacts`:
- Client artifact URI is `mlflow-artifacts://...` (resolved through the tracking server)
- Client uploads/downloads artifacts via `http://localhost:5000/api/2.0/mlflow-artifacts/...`
- No host paths are stored in metadata — portability guaranteed
- Multiple readers/writers possible (SQLite WAL mode)
- Same `MLFLOW_TRACKING_URI=http://localhost:5000` works for both local and containerized clients

---

## Implementation Plan

### Step 1: Update `docker-compose.yml`

```yaml
mlflow:
  volumes:
    - ./mlflow_data:/app/mlflow_data
  command: >
    mlflow server
    --host 0.0.0.0
    --port 5000
    --backend-store-uri sqlite:////app/mlflow_data/mlflow.db
    --default-artifact-root /app/mlflow_data/artifacts
    --serve-artifacts
```

Remove:
- `./mlruns:${PWD}/mlruns` volume (no longer needed)
- `MLFLOW_TRACKING_URI=${PWD}/mlruns` environment overrides in api/streamlit

Add:
- `MLFLOW_TRACKING_URI=http://mlflow:5000` for api and streamlit containers
  (they talk to the server, not the filesystem)

### Step 2: Update local training config (`.env`)

```
MLFLOW_TRACKING_URI=http://localhost:5000
```

Local training sends artifacts over HTTP to the MLflow container.
No fallback to filesystem needed — if the server is down, training fails fast
with a clear error rather than silently writing to an incompatible location.

### Step 3: Update `src/workflows/train.py`

Remove the HTTP-check fallback. If the server is not reachable, raise immediately.
Add a clear error: "Start MLflow first with: docker compose up -d mlflow".

### Step 4: Update `src/serving/api.py` and `streamlit_app/app.py`

Remove the localhost fallback. With `MLFLOW_TRACKING_URI=http://mlflow:5000`
(set via docker-compose environment), the model loads from the server.
The server serves artifacts via HTTP — no filesystem access needed.

### Step 5: Update `Makefile`

Remove `export UID` / `export GID` workaround (no longer needed for mlruns).
Keep `user:` in docker-compose to prevent root-owned files in `./data`.

### Step 6: Add `mlflow_data/` to `.gitignore`

```
/mlflow_data/
```

### Step 7: Remove `CLAUDE.md` `HOST_PROJECT_ROOT` references

---

## Migration Note

After switching to SQLite backend, **existing mlruns/ data is incompatible**.
You must retrain from scratch:

```bash
docker compose down
rm -rf mlruns/ mlflow_data/
make fetch-data
make train
make serve-docker
```

---

## Trade-offs

| Aspect | Filesystem (current) | SQLite + serve-artifacts (target) |
|--------|---------------------|----------------------------------|
| Portability | Requires `${PWD}` hack | Fully portable |
| MLflow version | Deprecated in 3.x | Recommended in 3.x |
| Local training | Needs `${PWD}` to match | Just needs HTTP server running |
| Concurrent access | Single writer only | SQLite WAL: multiple readers |
| Cloud migration | Complex (absolute paths) | Easy (change artifact root URL) |
| Setup complexity | Low (just files) | Low (single SQLite file) |
