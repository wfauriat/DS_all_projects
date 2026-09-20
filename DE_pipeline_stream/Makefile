# ─────────────────────────────────────────────────────────────────────────────
# Makefile: the single entry point for everything you run by hand.
#
#   make help      list every target, grouped by section
#
# Two families of targets:
#   local  → the uv-managed ./.venv (tests, lint, running a service on the host)
#   stack  → docker compose (grows layer by layer: source, kafka, spark, airflow)
# The source controls are plain curl calls to the source's admin API. Read
# them to see the HTTP contract; `jq` only formats the answers.
#
# .env is included, so make and docker compose see the same ports and settings.
# ─────────────────────────────────────────────────────────────────────────────
SHELL := /bin/bash
.DEFAULT_GOAL := help

# If a venv is activated globally (e.g. /opt/venvs/pyDS), uv warns and other
# tools can pick up the wrong interpreter. uv always uses the project's .venv,
# so recipes run without it.
unexport VIRTUAL_ENV

# Optional include: .env does not exist until `make setup` has run once.
-include .env
export

# Optional parts of the stack (compose profiles). Default: all of them.
# Override in .env, or per command: `COMPOSE_PROFILES= make up` = core only.
COMPOSE_PROFILES ?= stream,batch

# By default BuildKit attaches a timestamped provenance attestation to every
# image. The image ID then changes on every build, even when every layer comes
# from cache, and `make up` would needlessly restart the containers.
# (Run `docker compose up --build` by hand and you will see those restarts.)
export BUILDX_NO_DEFAULT_ATTESTATIONS := 1

UV       ?= uv
COMPOSE  ?= docker compose
API      := http://localhost:$(or $(SOURCE_API_PORT),8000)
CURL     := curl -fsS
JSON     := -H 'content-type: application/json'

.PHONY: help setup dirs test lint fmt up down ps logs smoke reset \
        source-run source-reset clock speed pause resume ff stream stats \
        faults fault fault-rate fault-on fault-off fault-log \
        topics tail dlq bridge-run bridge-reset alerts lake spark-reset \
        dags runs trigger warehouse sql dbt dbt-docs scorecard quality serving dashboard

help: ## list targets
	@awk 'BEGIN {FS = ":.*?## "} \
	  /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} \
	  /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Local environment
setup: .env dirs ## create .venv (uv sync), .env (with your UID/GID) and data/ dirs
	$(UV) sync

# .env is generated once from the template, with your UID/GID filled in.
# Edit .env afterwards; it is never overwritten.
.env:
	sed -e "s/^HOST_UID=.*/HOST_UID=$$(id -u)/" \
	    -e "s/^HOST_GID=.*/HOST_GID=$$(id -g)/" .env.example > .env
	@echo "created .env from .env.example"

# Runtime data that containers bind-mount (gitignored).
dirs:
	@mkdir -p data/source data/bridge data/warehouse data/lake data/checkpoints

test: ## run unit tests (pytest, all workspace members)
	$(UV) run pytest

lint: ## ruff lint + format check
	$(UV) run ruff check . && $(UV) run ruff format --check .

fmt: ## ruff autofix + format
	$(UV) run ruff check --fix . && $(UV) run ruff format .

##@ Stack (docker compose)
up: .env dirs ## build and start the stack in the background
	$(COMPOSE) up -d --build

# --profile '*': stop the services of every profile, even ones not currently selected.
down: ## stop the stack (data/ is kept)
	$(COMPOSE) --profile '*' down

ps: ## list containers and their health (one-shot ones too: kafka-init)
	$(COMPOSE) ps -a

logs: ## follow logs: make logs [s=source-api]
	$(COMPOSE) logs -f --tail=100 $(s)

# Checks the RUNNING stack, hop by hop (scripts/smoke.py). It forces two faults and
# triggers two DAG runs, but never starts or resets anything. From an empty stack:
# make reset && make up && make smoke
smoke: ## end-to-end check of the running stack: forces a teleport and a schema drift, follows them to the marts
	@PYTHONPATH=orchestration/include $(UV) run python scripts/smoke.py

# The pieces of state must stay consistent with each other. The bridge's
# checkpoint is a seq of the source's stream, Kafka holds what was sent up to
# it, and Spark's checkpoints and the warehouse's stored offsets are positions
# in those Kafka topics. So reset them together.
reset: ## stop everything and wipe ALL state: world, checkpoints, topics, lake, warehouse, Airflow DB
	$(COMPOSE) --profile '*' down -v
	rm -f data/source/state.* data/bridge/checkpoint.*
	rm -rf data/checkpoints/* data/lake/* data/warehouse/*

##@ Source: the synthetic bike-share API  (docs: http://localhost:8000/docs)
source-run: .env dirs ## run the source on the host instead of Docker (Ctrl-C stops it)
	SOURCE_PORT=$(or $(SOURCE_API_PORT),8000) $(UV) run python -m bikeshare_sim

source-reset: ## stop the source and wipe its state (a fresh world; see also `make reset`)
	-$(COMPOSE) rm -sf source-api
	rm -f data/source/state.pkl data/source/state.pkl.bak data/source/state.tmp

clock: ## show the simulated clock (sim_time, engine backlog, speed)
	@$(CURL) $(API)/admin/clock | jq .

speed: ## change speed: make speed x=600  (simulated seconds per real second)
	@$(CURL) -X PATCH $(API)/admin/clock $(JSON) -d '{"speed": $(x)}' | jq .

pause: ## freeze simulated time
	@$(CURL) -X PATCH $(API)/admin/clock $(JSON) -d '{"paused": true}' | jq .

resume: ## unfreeze simulated time
	@$(CURL) -X PATCH $(API)/admin/clock $(JSON) -d '{"paused": false}' | jq .

ff: ## fast-forward: make ff h=6 (or m=30). The skipped period arrives as a burst.
	@$(CURL) -X POST $(API)/admin/clock/advance $(JSON) \
	  -d '{"hours": $(or $(h),0), "minutes": $(or $(m),0)}' | jq .

stream: ## watch the raw SSE stream: make stream [types=trip_started,trip_ended]
	curl -sN "$(API)/v1/stream$(if $(types),?types=$(types))"

stats: ## counters: emitted events by type, injected faults, engine internals
	@$(CURL) $(API)/admin/stats | jq .

# jq program for `make faults`: one tab-separated line per fault. It lives in a
# variable because a backslash-newline inside a quoted recipe string would reach
# jq verbatim.
FAULTS_JQ := .[] | [.name, .kind, "enabled=\(.enabled)", "rate=\(.rate)", "injected=\(.injected)", \
             (.active_episodes | map(.station_id // "whole stream") | join(","))] | @tsv

faults: ## list faults: kind, enabled, rate, injected count, active episodes
	@$(CURL) $(API)/admin/faults | jq -r '$(FAULTS_JQ)' | column -t -s $$'\t'

fault: ## force one fault now: make fault f=teleport [station=ST-007]
	@$(CURL) -X POST $(API)/admin/faults/$(f)/trigger $(JSON) \
	  -d '$(if $(station),{"station_id": "$(station)"},{})' | jq .

fault-rate: ## change a fault's rate: make fault-rate f=duplicate r=0.05
	@$(CURL) -X PATCH $(API)/admin/faults/$(f) $(JSON) -d '{"rate": $(r)}' | jq .

fault-on: ## enable a fault: make fault-on f=duplicate
	@$(CURL) -X PATCH $(API)/admin/faults/$(f) $(JSON) -d '{"enabled": true}' | jq .

fault-off: ## disable a fault: make fault-off f=duplicate
	@$(CURL) -X PATCH $(API)/admin/faults/$(f) $(JSON) -d '{"enabled": false}' | jq .

fault-log: ## the last 20 injected faults (ground truth)
	@$(CURL) "$(API)/admin/fault-log?limit=10000" \
	  | jq -c '.[-20:][] | {fault_id, fault_type, injected_at, event_id, details}'

##@ Kafka and the bridge  (Console UI: http://localhost:8081)
# The Kafka CLI tools ship inside the broker image. Run them there, against the INTERNAL listener.
KAFKA_BIN := $(COMPOSE) exec -T kafka /opt/kafka/bin

topics: ## topics with partition count and messages written (end offsets)
	@$(KAFKA_BIN)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --exclude-internal-topics \
	  | awk -F: '{n[$$1] += $$3; p[$$1]++} \
	    END {for (t in n) printf "  %-30s partitions=%-2d messages=%d\n", t, p[t], n[t]}' | sort

tail: ## print messages: make tail t=bikeshare.trip-events.v1 [n=5] [from=beginning]
	@$(KAFKA_BIN)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $(t) \
	  --max-messages $(or $(n),5) $(if $(from),--from-beginning) --timeout-ms 20000 \
	  --command-property enable.auto.commit=false \
	  --formatter-property print.partition=true --formatter-property print.key=true \
	  --formatter-property print.headers=true 2>/dev/null || true

dlq: ## the first dead letters (events the bridge rejected): make dlq [n=3]
	@$(MAKE) -s tail t=bikeshare.dlq.v1 n=$(or $(n),3) from=beginning

# The container bridge must be stopped first: both would share data/bridge/checkpoint.json.
bridge-run: .env dirs ## run the bridge on the host (first: docker compose stop bridge)
	SOURCE_URL=$(API) KAFKA_BOOTSTRAP=localhost:$(or $(KAFKA_HOST_PORT),9094) \
	  BRIDGE_STATE_DIR=data/bridge $(UV) run python -m bikeshare_bridge

bridge-reset: ## stop the bridge and forget its checkpoint (next start replays the source buffer)
	-$(COMPOSE) rm -sf bridge
	rm -f data/bridge/checkpoint.json

##@ Spark analyzer  (Spark UI: http://localhost:4040)
alerts: ## alerts raised by Spark: counts by type and the latest ones (a host Kafka consumer)
	@$(UV) run python scripts/peek_alerts.py

lake: ## Spark's Parquet output (station metrics), read in place by DuckDB from the host
	@$(UV) run python scripts/peek_lake.py

# Resetting a job means resetting its progress AND its outputs together:
# replaying into a lake and topic that already hold the old results duplicates them.
# Downstream too: the warehouse's stored positions number the messages of the DELETED
# alerts topic, and the recreated one starts over at offset 0. So the alerts landed
# from it are forgotten with their positions, once Kafka confirms the topic is gone.
spark-reset: ## stop Spark, wipe its checkpoints, lake, alerts topic and the alerts landed from it (replays from the earliest offsets)
	-$(COMPOSE) rm -sf spark
	-$(KAFKA_BIN)/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic bikeshare.alerts.v1
	rm -rf data/checkpoints/* data/lake/*
	@PYTHONPATH=orchestration/include $(UV) run python scripts/forget_landed_topic.py bikeshare.alerts.v1
	@echo "next: make up (kafka-init re-creates the alerts topic, Spark starts over, the next landing re-lands it)"

##@ Airflow and the warehouse  (Airflow UI: http://localhost:8080)
# The Airflow CLI runs inside the scheduler container, against the metadata database.
# Through /entrypoint: `docker compose exec` skips the image's entrypoint, and
# without it Python can't find Airflow when the container runs as your UID.
AIRFLOW := $(COMPOSE) exec -T airflow-scheduler /entrypoint airflow

dags: ## DAGs, with paused state and import errors (if any)
	@$(AIRFLOW) dags list -o table 2>/dev/null
	@$(AIRFLOW) dags list-import-errors 2>/dev/null | grep -v "^No data found" || true

runs: ## latest runs of a DAG: make runs [d=stream_landing]
	@$(AIRFLOW) dags list-runs $(or $(d),stream_landing) -o table 2>/dev/null | head -12

trigger: ## run a DAG now, outside its schedule: make trigger d=api_extract
	@$(AIRFLOW) dags trigger $(d) -o table 2>/dev/null

# PYTHONPATH mirrors the Airflow containers, so the script uses the same `landing` package.
warehouse: ## what landed in DuckDB: rows per raw table, loader positions vs Kafka, last batches
	@PYTHONPATH=orchestration/include $(UV) run python scripts/peek_warehouse.py

# harlequin on the SERVING copy (layer 5): the marts, never locked by a DAG.
# For raw/staging, point it at data/warehouse/bikeshare.duckdb instead, which
# DuckDB refuses while a task writes (retry).
sql: ## explore the marts in harlequin (a terminal SQL IDE), on the serving copy
	$(UV) run harlequin --read-only data/warehouse/bikeshare_serving.duckdb

##@ dbt and the serving copy
# dbt on the host, from transform/: the same dbt version as in Airflow.
# Its artifacts and logs never go to target/ and logs/, which belong to Airflow's
# dbt: quality_report reads target/run_results.json right after the build, and
# two dbt processes sharing a folder overwrite each other's results and parse cache.
DBT := DBT_PROFILES_DIR=. DBT_SEND_ANONYMOUS_USAGE_STATS=false DBT_LOG_PATH=logs-host $(UV) run dbt

# `make dbt` works on the live warehouse. It needs the write lock, so it fails if a
# DAG task is writing at that moment. Run it again, or pause the DAGs in the UI while you iterate.
dbt: ## run dbt on the host: make dbt c="build -s staging"  (default: build)
	cd transform && DBT_TARGET_PATH=target-host $(DBT) $(or $(c),build)

# The docs never open the live warehouse: the catalog is read from a copy of its
# structure (no rows), taken in well under a second while no task writes.
DOCS_DIR := target-docs
dbt-docs: ## dbt docs with the lineage graph, raw → marts: http://localhost:8082
	@PYTHONPATH=orchestration/include $(UV) run python scripts/copy_warehouse_schema.py transform/$(DOCS_DIR)/bikeshare.duckdb
	cd transform && DBT_TARGET_PATH=$(DOCS_DIR) DUCKDB_PATH=$(DOCS_DIR)/bikeshare.duckdb $(DBT) docs generate
	cd transform && DBT_TARGET_PATH=$(DOCS_DIR) $(DBT) docs serve --port 8082 --no-browser

# These read the serving copy, which Airflow republishes after every dbt build.
scorecard: ## precision and recall of every detector, against the ground truth
	@$(UV) run python scripts/peek_serving.py scorecard

quality: ## data quality per simulated day
	@$(UV) run python scripts/peek_serving.py quality

serving: ## what the serving copy holds, and when it was published
	@$(UV) run python scripts/peek_serving.py published

# Streamlit on the host. It reads the serving copy only, and follows each new publish.
dashboard: ## Streamlit dashboard on the serving copy: http://localhost:8501 (Ctrl-C stops it)
	$(UV) run streamlit run dashboard/app.py --server.port $(or $(DASHBOARD_PORT),8501) \
	  --server.address localhost --server.headless true --browser.gatherUsageStats false
