"""DAG `stream_landing`: Kafka topics → DuckDB `raw` tables, every 5 minutes, exactly once.

    prepare_warehouse ─┬─► land_trip_events ────┐
                       ├─► land_station_status ─┤
                       ├─► land_dlq ────────────┼─► report ──► Asset `raw_stream`
                       └─► land_alerts ─────────┘              (layer 5: triggers dbt)

A stream never ends, but a DAG run must. Each landing task takes a BOUNDED slice
of its topic and stores the rows together with the new Kafka position, in one
DuckDB transaction (include/landing/kafka_loader.py explains why that is
exactly-once).

Wiring, all set as environment variables in docker-compose.yml (x-airflow-common):
    Connection  kafka_landing   AIRFLOW_CONN_KAFKA_LANDING   bootstrap servers (kafka:9092)
    Variable    warehouse_path  AIRFLOW_VAR_WAREHOUSE_PATH   the DuckDB file
    Pool        duckdb, 1 slot  created by airflow-init       DuckDB allows one writer at a time

Two clocks: Airflow schedules on wall-clock time, while the data inside is
simulated. At 60× speed, a 5-minute interval lands about 5 simulated hours.
"""

import logging
from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, BaseHook, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from landing import kafka_loader, warehouse

log = logging.getLogger(__name__)

# Downstream DAGs schedule on this asset instead of a clock (data-aware scheduling).
RAW_STREAM = Asset(name="raw_stream", uri="duckdb://warehouse/raw/stream")


@dag(
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,  # don't backfill every 5-min interval since 2026-01-01
    max_active_runs=1,  # runs never overlap: each one starts where the previous stopped
    is_paused_upon_creation=False,  # start running as soon as the DAG is parsed
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["landing", "kafka", "duckdb"],
    doc_md=__doc__,
)
def stream_landing():
    @task(pool="duckdb")
    def prepare_warehouse() -> None:
        with warehouse.connect(Variable.get("warehouse_path")) as conn:
            warehouse.ensure_raw_schema(conn)

    def land(topic: str, table: str):
        @task(task_id=f"land_{table}", pool="duckdb")
        def land_topic() -> dict:
            bootstrap = BaseHook.get_connection("kafka_landing").extra_dejson["bootstrap.servers"]
            consumer = kafka_loader.make_consumer(bootstrap)
            with warehouse.connect(Variable.get("warehouse_path")) as conn:
                return kafka_loader.land_topic(
                    conn, consumer, topic, run_id=get_current_context()["run_id"]
                )

        return land_topic()

    @task(outlets=[RAW_STREAM])
    def report(summaries: list[dict]) -> None:
        total = sum(s["rows"] for s in summaries)
        for s in summaries:
            log.info("%-22s %7d rows  offsets %s  %s", s["table"], s["rows"], s["partitions"],
                     "; ".join(s["notes"]))  # fmt: skip
        if total == 0:
            # A skipped task emits no asset event, so nothing downstream runs for nothing.
            raise AirflowSkipException("no new messages in any topic")
        get_current_context()["outlet_events"][RAW_STREAM].extra = {
            "rows": total,
            "by_table": {s["table"]: s["rows"] for s in summaries},
        }

    landed = [land(topic, table) for topic, table in warehouse.TOPIC_TABLES.items()]
    prepare_warehouse() >> landed
    report(landed)


stream_landing()
