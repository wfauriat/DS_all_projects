.PHONY: setup test clean \
        local-mlflow local-fetch local-train local-serve local-dashboard local-monitor \
        docker-fetch docker-train docker-serve docker-monitor

export UID := $(shell id -u)
export GID := $(shell id -g)

# ── SETUP ──────────────────────────────────────────────────────────────────

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	cp -n .env.example .env || true
	@echo "Setup complete. Edit .env with your EIA API key."

# ── LOCAL MODE ─────────────────────────────────────────────────────────────
# Everything runs in the .venv Python environment on the host. No Docker needed
# for individual steps, but MLflow must be running first (see local-mlflow).
#
# Typical workflow:
#   terminal 1: make local-mlflow
#   terminal 2: make local-fetch && make local-train && make local-serve
#               make local-dashboard  (optional, in a third terminal)

local-mlflow:
	@if curl -sf http://localhost:5000/health > /dev/null 2>&1; then \
		echo "MLflow already running at http://localhost:5000 — nothing to do."; \
	else \
		mkdir -p mlflow_data && \
		.venv/bin/mlflow server \
			--host 0.0.0.0 --port 5000 \
			--backend-store-uri sqlite:///$(CURDIR)/mlflow_data/mlflow.db \
			--artifacts-destination $(CURDIR)/mlflow_data/artifacts \
			--serve-artifacts \
			--allowed-hosts localhost,localhost:5000; \
	fi

local-fetch:
	.venv/bin/python -m src.workflows.ingest

local-train:
	.venv/bin/python -m src.workflows.train

local-serve:
	.venv/bin/uvicorn src.serving.api:app --port 8000

local-dashboard:
	.venv/bin/streamlit run streamlit_app/app.py

local-monitor:
	.venv/bin/python -m src.workflows.monitor

# ── DOCKER MODE ────────────────────────────────────────────────────────────
# Everything runs inside Docker containers. MLflow, API, and Streamlit are all
# containerized.
# - Raw data and features (./data/) are stored on the host as a bind mount.
# - MLflow DB and model artifacts are stored in a Docker-managed named volume
#   (mlflow_data). This is separate from the local mode's ./mlflow_data/ dir.
#
# Typical workflow:
#   make docker-fetch && make docker-train && make docker-serve

docker-fetch:
	mkdir -p data
	docker compose run --rm ingest

docker-train:
	mkdir -p data
	docker compose up -d --wait mlflow
	docker compose run --rm train
	docker compose stop mlflow

docker-serve:
	docker compose up --build api streamlit mlflow

docker-monitor:
	docker compose run --rm monitor
	@if [ -f data/retrain_needed ]; then \
		echo "Drift detected — triggering retraining..."; \
		$(MAKE) docker-train; \
		rm -f data/retrain_needed; \
	else \
		echo "No drift detected, no retraining needed."; \
	fi

# ── UTILITIES ──────────────────────────────────────────────────────────────

test:
	.venv/bin/python -m pytest tests/ -v

clean:
	docker compose down -v 2>/dev/null || true
	rm -rf data/ mlruns/ mlflow_data/ __pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
