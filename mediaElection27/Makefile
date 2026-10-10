.PHONY: help venv install db-up db-down db-logs psql migrate seed ingest-once extract extract-all api dev test lint typecheck format clean frontend-install frontend-dev frontend-build frontend-typecheck

PY := .venv/bin/python
PIP := .venv/bin/pip
ALEMBIC := .venv/bin/alembic
PYTEST := .venv/bin/pytest
RUFF := .venv/bin/ruff
MYPY := .venv/bin/mypy

help:  ## Show available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*##"} {printf "  \033[1;36m%-15s\033[0m %s\n", $$1, $$2}'

venv:  ## Create local .venv (Python 3.12)
	python3.12 -m venv .venv
	$(PIP) install -U pip

install: venv  ## Install project + dev deps into .venv
	$(PIP) install -e ".[dev]"

db-up:  ## Start Postgres in docker
	docker compose up -d postgres
	@echo "Waiting for Postgres to be ready..."
	@until docker compose exec -T postgres pg_isready -U media27 -d media27 >/dev/null 2>&1; do sleep 1; done
	@echo "Postgres ready on localhost:5432"

db-down:  ## Stop Postgres
	docker compose down

db-logs:  ## Tail Postgres logs
	docker compose logs -f postgres

psql:  ## Open an interactive psql shell against the running Postgres
	docker compose exec postgres psql -U media27 -d media27

migrate:  ## Apply alembic migrations
	$(ALEMBIC) upgrade head

seed:  ## Seed candidates and sources from YAML
	$(PY) -m app.sources.seed

ingest-once: ## Run one ingest pass, then keyword-extract any new articles
	$(PY) -m app.ingest.run --once
	$(PY) -m app.extract.run

extract:  ## Run keyword extractor over articles that don't yet have mentions
	$(PY) -m app.extract.run

extract-all:  ## Reprocess every article (useful after editing aliases)
	$(PY) -m app.extract.run --all

api:  ## Run FastAPI locally
	$(PY) -m uvicorn app.api.main:app --reload --port 8000

frontend-install:  ## Install frontend npm deps
	cd frontend && npm install

frontend-dev:  ## Run Vite dev server (http://localhost:5173)
	cd frontend && npm run dev

frontend-build:  ## Build frontend for production into frontend/dist
	cd frontend && npm run build

frontend-typecheck:  ## Type-check the frontend with tsc --noEmit
	cd frontend && npm run typecheck

dev: db-up migrate seed  ## Bring up DB, migrate, seed (full local bootstrap)

test:  ## Run pytest
	$(PYTEST) -v

lint:  ## Lint with ruff
	$(RUFF) check app tests

format:  ## Format with ruff
	$(RUFF) format app tests

typecheck:  ## Type check with mypy --strict
	$(MYPY) app

clean:  ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
