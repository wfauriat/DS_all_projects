"""End-to-end smoke test of the RUNNING stack: two forced faults, followed through every hop.

    make smoke                              (= PYTHONPATH=orchestration/include uv run python scripts/smoke.py)
    make reset && make up && make smoke     from an empty stack

It never starts, stops or resets anything. It does leave traces, like any user of
the stack would: two faults forced on the source (a teleport and a schema drift,
both recorded in the ground truth, so the scorecard stays honest), and one run
of `stream_landing` and `api_extract` triggered outside their schedules.

The checks run in pipeline order and stop at the first failure, because each hop
feeds the next:

    1 source     healthy, simulated time not paused
    2 kafka      new events in both event topics
    3 bridge     the forced schema drift reaches the dead-letter queue, keyed by its event_id
    4 spark      all four queries commit batches; the forced teleport raises an alert
    5 airflow    no import errors, DAGs unpaused; landing + extract succeed, dbt_transform follows
    6 warehouse  the teleport's alert, the drift's dead letter and both faults landed in raw;
                 dbt flagged the trip in fct_trips; the serving copy was republished with
                 non-empty marts and scorecard

What it does NOT check: the scorecard's precision and recall for these two faults.
The scorecard judges only faults older than 4 simulated hours (TOUR.md §6), which
would add minutes; step 6 checks the same matches row by row instead.

Without the `stream` or `batch` compose profile (COMPOSE_PROFILES), the steps
that need Spark or Airflow are skipped, and said so.
"""

import json
import os
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime

import duckdb
import httpx
from confluent_kafka import Consumer, KafkaException, TopicPartition

from landing import warehouse

SOURCE = f"http://localhost:{os.environ.get('SOURCE_API_PORT', '8000')}"
AIRFLOW = f"http://localhost:{os.environ.get('AIRFLOW_PORT', '8080')}/api/v2"
KAFKA = f"localhost:{os.environ.get('KAFKA_HOST_PORT', '9094')}"
PROFILES = set(os.environ.get("COMPOSE_PROFILES", "stream,batch").split(","))

WAREHOUSE = "data/warehouse/bikeshare.duckdb"
SERVING = "data/warehouse/bikeshare_serving.duckdb"
CHECKPOINTS = "data/checkpoints"
SPARK_QUERIES = ["reference_rules", "station_metrics", "station_health", "trip_pairing"]
EVENT_TOPICS = ["bikeshare.trip-events.v1", "bikeshare.station-status.v1"]
DLQ, ALERTS = "bikeshare.dlq.v1", "bikeshare.alerts.v1"


class Failed(Exception):
    pass


def wait_for(what: str, check: Callable, timeout_s: float, every_s: float = 2.0):
    """Poll `check` until it returns something truthy; fail with `what` after `timeout_s`."""
    deadline = time.monotonic() + timeout_s
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() > deadline:
            raise Failed(f"{what} (waited {timeout_s:.0f} s)")
        time.sleep(every_s)


class Report:
    def __init__(self) -> None:
        self.t0 = time.monotonic()

    def ok(self, step: str, detail: str) -> None:
        print(f"  ✓ {step:<10} {detail}  [{time.monotonic() - self.t0:.0f} s]", flush=True)

    def skip(self, step: str, why: str) -> None:
        print(f"  - {step:<10} skipped: {why}", flush=True)


# ── Kafka helpers (host listener) ───────────────────────────────────────────
def consumer() -> Consumer:
    return Consumer({"bootstrap.servers": KAFKA, "group.id": "smoke", "enable.auto.commit": False})


def end_offsets(c: Consumer, topic: str) -> dict[int, int]:
    partitions = c.list_topics(topic, timeout=10).topics[topic].partitions
    return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}


def find_message(topic: str, since: dict[int, int], match: Callable, timeout_s: float):
    """The first message after the `since` offsets for which match(key, value) is true."""
    c = consumer()
    c.assign([TopicPartition(topic, p, o) for p, o in since.items()])
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode() if msg.key() else None
            value = json.loads(msg.value())
            if match(key, value):
                return value
    finally:
        c.close()
    raise Failed(f"no matching message on {topic} (waited {timeout_s:.0f} s)")


# ── Source helpers ──────────────────────────────────────────────────────────
def last_fault_id(api: httpx.Client) -> int:
    last = 0
    while page := api.get("/admin/fault-log", params={"after_id": last, "limit": 10_000}).json():
        last = page[-1]["fault_id"]
    return last


def force_fault(api: httpx.Client, name: str, after_id: int) -> dict:
    """Arm a fault, then wait for the source to inject it (per-event faults need an eligible event)."""
    api.post(f"/admin/faults/{name}/trigger", json={}).raise_for_status()

    def injected():
        faults = api.get("/admin/fault-log", params={"after_id": after_id, "limit": 10_000}).json()
        return next((f for f in faults if f["fault_type"] == name), None)

    return wait_for(f"the source never injected the forced {name}", injected, 60, every_s=1)


# ── Airflow helpers (REST API, no login locally) ────────────────────────────
def trigger(af: httpx.Client, dag_id: str) -> str:
    r = af.post(f"/dags/{dag_id}/dagRuns", json={"logical_date": None})
    r.raise_for_status()
    return r.json()["dag_run_id"]


def run_finished(af: httpx.Client, dag_id: str, run_id: str) -> Callable:
    def check():
        run = af.get(f"/dags/{dag_id}/dagRuns/{run_id}").json()
        if run["state"] == "failed":
            raise Failed(f"{dag_id} run {run_id} failed: see its task logs in the Airflow UI")
        return run if run["state"] == "success" else None

    return check


def main() -> None:
    started = datetime.now(UTC)
    report = Report()
    print(f"smoke test of the running stack (profiles: {','.join(sorted(PROFILES)) or 'core'})")
    api = httpx.Client(base_url=SOURCE, timeout=15)

    # 1 ── source ─────────────────────────────────────────────────────────────
    def healthy():  # right after `make up`, the source may still be starting
        try:
            return api.get("/health").is_success
        except httpx.HTTPError:
            return False

    wait_for(f"source API not healthy at {SOURCE}: make up, then make ps", healthy, 60)
    clock = api.get("/admin/clock").json()
    if clock["paused"]:
        raise Failed("simulated time is paused, so no events flow: make resume")
    report.ok("source", f"healthy, sim time {clock['sim_time'][:16]}, speed {clock['speed']:g}×")

    # 2 ── kafka ──────────────────────────────────────────────────────────────
    c = consumer()
    before = {t: sum(end_offsets(c, t).values()) for t in EVENT_TOPICS}
    dlq_since, alerts_since = end_offsets(c, DLQ), end_offsets(c, ALERTS)

    def grown():
        now = {t: sum(end_offsets(c, t).values()) for t in EVENT_TOPICS}
        return now if all(now[t] > before[t] for t in EVENT_TOPICS) else None

    now = wait_for("no new messages in both event topics: is the bridge running?", grown, 60)
    c.close()
    report.ok("kafka", ", ".join(f"{t.split('.')[1]} +{now[t] - before[t]}" for t in EVENT_TOPICS))

    # 3 ── bridge: a forced schema drift is dead-lettered ─────────────────────
    after_id = last_fault_id(api)
    drift = force_fault(api, "schema_drift", after_id)
    dead = find_message(DLQ, dlq_since, lambda key, _: key == drift["event_id"], 60)
    report.ok(
        "bridge", f"schema drift ({drift['details'].get('variant')}) → DLQ: {dead['error_type']}"
    )

    # 4 ── spark: every query commits; a forced teleport raises an alert ──────
    teleport = force_fault(api, "teleport", after_id)
    trip_id = teleport["details"]["trip_id"]
    if "stream" in PROFILES:
        commits_at_start = {q: _last_commit(q) for q in SPARK_QUERIES}
        wait_for(
            "not every Spark query committed a new batch: make logs s=spark",
            lambda: all(_last_commit(q) > commits_at_start[q] for q in SPARK_QUERIES),
            120,
        )
        alert = find_message(
            ALERTS,
            alerts_since,
            lambda _, v: v["alert_type"] == "teleport" and v["entity_id"] == trip_id,
            120,
        )
        kmh = json.loads(alert["detail"])["kmh"]  # Spark writes detail as JSON text (to_json)
        report.ok("spark", f"4 queries committing; teleport alert for {trip_id} ({kmh} km/h)")
    else:
        report.skip("spark", "compose profile `stream` is off")

    if "batch" not in PROFILES:
        report.skip("airflow", "compose profile `batch` is off")
        report.skip("warehouse", "compose profile `batch` is off")
        print("smoke test passed (partial)")
        return

    # 5 ── airflow: landing and extract now, then dbt_transform on their assets ──
    af = httpx.Client(base_url=AIRFLOW, timeout=15)
    dags = ("stream_landing", "api_extract", "dbt_transform")

    def parsed():  # right after `make up`, the DAG processor needs a moment
        try:
            return all(af.get(f"/dags/{d}").is_success for d in dags)
        except httpx.HTTPError:
            return False

    wait_for("Airflow API down or DAGs not parsed yet: make ps, make dags", parsed, 180, 5)
    errors = af.get("/importErrors").json()["total_entries"]
    if errors:
        raise Failed(f"{errors} DAG import error(s): make dags")
    paused = [d for d in dags if af.get(f"/dags/{d}").json()["is_paused"]]
    if paused:
        raise Failed(f"paused DAG(s): {', '.join(paused)} (unpause them in the UI)")
    runs = {d: trigger(af, d) for d in ("stream_landing", "api_extract")}
    ended = [
        wait_for(f"{d} run did not finish", run_finished(af, d, r), 300, every_s=3)["end_date"]
        for d, r in runs.items()
    ]

    def dbt_after_landing():
        # the run created by the LATER asset event starts after both landing runs ended
        latest = af.get(
            "/dags/dbt_transform/dagRuns",
            params={"order_by": "-start_date", "limit": 1, "start_date_gte": max(ended)},
        ).json()["dag_runs"]
        return latest and run_finished(af, "dbt_transform", latest[0]["dag_run_id"])()

    dbt_run = wait_for(
        "no dbt_transform run succeeded after the landing", dbt_after_landing, 420, 5
    )
    report.ok(
        "airflow", f"stream_landing + api_extract triggered; dbt_transform {dbt_run['run_type']} ok"
    )

    # 6 ── warehouse and serving copy ─────────────────────────────────────────
    with warehouse.connect(WAREHOUSE, read_only=True) as conn:
        landed = conn.execute(
            """
            SELECT
              -- parentheses: DuckDB's ->> binds more loosely than = and AND
              (SELECT count(*) FROM raw.alerts WHERE (value->>'$.alert_type') = 'teleport'
                                                 AND (value->>'$.entity_id') = ?),
              (SELECT count(*) FROM raw.dlq WHERE msg_key = ?),
              (SELECT count(*) FROM raw.fault_log WHERE fault_id IN (?, ?))""",
            [trip_id, drift["event_id"], teleport["fault_id"], drift["fault_id"]],
        ).fetchone()
    alerts_landed, dead_letters_landed, faults_landed = landed
    missing = [
        name
        for name, ok in [
            ("Spark's teleport alert in raw.alerts", alerts_landed or "stream" not in PROFILES),
            ("the dead letter in raw.dlq", dead_letters_landed),
            ("both faults in raw.fault_log", faults_landed == 2),
        ]
        if not ok
    ]
    if missing:
        raise Failed("not landed: " + "; ".join(missing))

    serving = duckdb.connect(SERVING, read_only=True)  # a new file after each publish: open it now
    [republished] = serving.execute(
        "SELECT published_at >= ?::TIMESTAMPTZ FROM main.published", [started.isoformat()]
    ).fetchone()
    [flagged] = serving.execute(
        "SELECT count(*) FROM marts.fct_trips WHERE trip_id = ? AND is_implausible_speed", [trip_id]
    ).fetchone()
    marts = [t for (t,) in serving.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name = 'marts'").fetchall()]  # fmt: skip
    rows = {t: serving.execute(f"SELECT count(*) FROM marts.{t}").fetchone()[0] for t in marts}
    serving.close()
    if not republished:
        raise Failed("the serving copy was not republished after the dbt run")
    if not flagged:
        raise Failed(f"trip {trip_id} is not flagged is_implausible_speed in marts.fct_trips")
    empty = [t for t, n in rows.items() if n == 0]
    if not marts or empty:
        raise Failed(f"serving copy: {len(marts)} marts, empty: {', '.join(empty) or '-'}")
    report.ok(
        "warehouse",
        f"alert, dead letter and faults landed; {trip_id} flagged in fct_trips; serving copy "
        f"republished, {len(marts)} marts ({rows['mart_detection_scorecard']} scorecard rows)",
    )
    print("smoke test passed")


def _last_commit(query: str) -> int:
    """The id of the query's latest committed micro-batch (-1 if none): its checkpoint's commits/ files."""
    path = os.path.join(CHECKPOINTS, query, "commits")
    ids = [int(f) for f in os.listdir(path) if f.isdigit()] if os.path.isdir(path) else []
    return max(ids, default=-1)


if __name__ == "__main__":
    try:
        main()
    except (Failed, httpx.HTTPError, KafkaException, duckdb.Error) as exc:
        # a service that is down or refuses: one line, not a traceback
        print(f"  ✗ {type(exc).__name__ + ': ' if not isinstance(exc, Failed) else ''}{exc}")
        print("smoke test FAILED")
        sys.exit(1)
