"""DAG `dbt_transform`: raw → staging → marts with dbt, whenever new data has landed.

    Asset raw_stream ─┐
                      ├─► source_freshness ─► dbt_build ─► quality_report ─┐
    Asset raw_api ────┘                              └──────────────────────┴─► publish_serving ──► Asset serving_marts

Data-aware scheduling: this DAG has no clock. It runs when either asset receives
an event, i.e. after a landing run that actually brought new rows (stream_landing
emits no event when it landed nothing).

dbt runs from its own virtualenv (/opt/dbt-venv, see orchestration/Dockerfile) on
the project mounted at /opt/airflow/transform. What dbt reads (docker-compose.yml):
    DBT_PROFILES_DIR  /opt/airflow/transform      → transform/profiles.yml
    DUCKDB_PATH       the warehouse file          (profiles.yml)
    LAKE_DIR          Spark's Parquet lake        (models/sources.yml)
Everything writing to the warehouse runs in the `duckdb` pool (1 slot), shared
with the landing DAGs.
"""

import json
import logging
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Variable, dag, get_current_context, task

from landing import warehouse
from serving import publish

log = logging.getLogger(__name__)

# The same name + uri as in stream_landing.py and api_extract.py make them the same assets.
RAW_STREAM = Asset(name="raw_stream", uri="duckdb://warehouse/raw/stream")
RAW_API = Asset(name="raw_api", uri="duckdb://warehouse/raw/api")
SERVING_MARTS = Asset(name="serving_marts", uri="duckdb://serving/marts")

DBT_PROJECT = Path("/opt/airflow/transform")
DBT = f"cd {DBT_PROJECT} && /opt/dbt-venv/bin/dbt"


@dag(
    schedule=(RAW_STREAM | RAW_API),  # an asset expression: either one triggers a run
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["transform", "dbt", "duckdb"],
    doc_md=__doc__,
)
def dbt_transform():
    @task.bash(pool="duckdb")
    def source_freshness() -> str:
        # Is data still arriving? Fails if a source is older than its error_after (models/sources.yml).
        return f"{DBT} source freshness"

    # all_done: build even if freshness failed. Stale data still deserves consistent marts.
    @task.bash(pool="duckdb", trigger_rule="all_done")
    def dbt_build() -> str:
        # seeds, snapshots, models and tests, in dependency order. Exit code 1 on any error.
        return f"{DBT} build"

    @task(pool="duckdb", trigger_rule="all_done")
    def quality_report() -> dict:
        """What dbt just did, in the task log: results by status, tests that found rows,
        source freshness, and the detection scorecard."""
        target = DBT_PROJECT / "target"
        results = json.loads((target / "run_results.json").read_text())["results"]
        by_status = Counter(f"{r['unique_id'].split('.')[0]}:{r['status']}" for r in results)
        log.info("dbt build: %s", dict(sorted(by_status.items())))
        for r in results:
            if r["status"] in ("warn", "fail", "error"):
                log.warning("%-5s %s (%s rows)", r["status"], r["unique_id"], r.get("failures"))

        sources = target / "sources.json"  # absent if the freshness task could not run at all
        freshness = json.loads(sources.read_text())["results"] if sources.exists() else []
        for s in freshness:
            log.info("freshness %-7s %s (%.0f s old)", s["status"], s["unique_id"],
                     s.get("max_loaded_at_time_ago_in_s") or 0)  # fmt: skip

        with warehouse.connect(Variable.get("warehouse_path"), read_only=True) as conn:
            rows = conn.execute(
                "SELECT fault_type, detector, check_name, precision, recall "
                "FROM marts.mart_detection_scorecard ORDER BY fault_type, detector"
            ).fetchall()
        for fault_type, detector, check, precision, recall in rows:
            log.info("scorecard %-17s %-6s %-18s precision=%s recall=%s",
                     fault_type, detector, check, precision, recall)  # fmt: skip
        return {"results": dict(by_status), "scorecard_rows": len(rows)}

    @task(pool="duckdb", outlets=[SERVING_MARTS])
    def publish_serving() -> dict:
        copied = publish.publish(Variable.get("warehouse_path"), Variable.get("serving_path"))
        get_current_context()["outlet_events"][SERVING_MARTS].extra = copied
        return copied

    build = dbt_build()
    report = quality_report()
    source_freshness() >> build >> report
    [build, report] >> publish_serving()  # publish only after a SUCCESSFUL build


dbt_transform()
