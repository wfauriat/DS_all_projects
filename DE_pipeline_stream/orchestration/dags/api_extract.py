"""DAG `api_extract`: the source's REST API → DuckDB `raw`, every 15 minutes.

    prepare_warehouse ─┬─► stations ──┐
                       ├─► bikes ─────┤
                       ├─► weather ───┼─► report ──► Asset `raw_api`
                       └─► fault_log ─┘

The batch side of the vendor: reference data (stations, bikes), hourly weather
and the ground-truth fault log. Each dataset has its own extraction pattern
(snapshot, or incremental by time or by id); include/landing/api_extract.py
explains which and why.

Wiring (environment variables in docker-compose.yml, x-airflow-common):
    Connection  bikeshare_api   AIRFLOW_CONN_BIKESHARE_API   {"host": "source-api", "port": 8000, …}
    Variable    warehouse_path  AIRFLOW_VAR_WAREHOUSE_PATH   the DuckDB file
    Pool        duckdb, 1 slot  created by airflow-init       shared with stream_landing
"""

import logging
from datetime import UTC, datetime, timedelta

import httpx
from airflow.sdk import Asset, BaseHook, Variable, dag, get_current_context, task

from landing import api_extract, warehouse

log = logging.getLogger(__name__)

RAW_API = Asset(name="raw_api", uri="duckdb://warehouse/raw/api")

EXTRACTS = {
    "stations": api_extract.extract_stations,
    "bikes": api_extract.extract_bikes,
    "weather": api_extract.extract_weather,
    "fault_log": api_extract.extract_fault_log,
}


def source_api() -> httpx.Client:
    conn = BaseHook.get_connection("bikeshare_api")
    # HTTP connections keep the protocol in the `schema` field (the HTTP provider's convention).
    return httpx.Client(base_url=f"{conn.schema or 'http'}://{conn.host}:{conn.port}", timeout=30)


@dag(
    dag_id="api_extract",  # the function name differs only to avoid clashing with landing.api_extract
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["landing", "api", "duckdb"],
    doc_md=__doc__,
)
def api_extract_dag():
    @task(pool="duckdb")
    def prepare_warehouse() -> None:
        with warehouse.connect(Variable.get("warehouse_path")) as conn:
            warehouse.ensure_raw_schema(conn)

    def extract(name: str, extract_fn):
        @task(task_id=name, pool="duckdb")
        def run_extract() -> dict:
            run_id = get_current_context()["run_id"]
            with source_api() as api, warehouse.connect(Variable.get("warehouse_path")) as conn:
                return {"dataset": name, "rows": extract_fn(conn, api, run_id=run_id)}

        return run_extract()

    @task(outlets=[RAW_API])
    def report(results: list[dict]) -> None:
        for r in results:
            log.info("raw.%-10s +%d rows", r["dataset"], r["rows"])
        get_current_context()["outlet_events"][RAW_API].extra = {
            r["dataset"]: r["rows"] for r in results
        }

    extracted = [extract(name, fn) for name, fn in EXTRACTS.items()]
    prepare_warehouse() >> extracted
    report(extracted)


api_extract_dag()
