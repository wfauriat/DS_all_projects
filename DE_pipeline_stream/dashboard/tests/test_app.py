"""The dashboard renders every tab from a small serving copy, and explains itself without one.

Streamlit's AppTest runs the script in-process, without a browser: it catches Python
errors and checks what the page says. The charts' rendering is not checked here.
"""

import os
from pathlib import Path

import duckdb
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).parents[1] / "app.py")

# A few rows per mart, with the columns the dashboard reads.
SERVING_COPY = [
    "CREATE SCHEMA marts",
    "CREATE TABLE main.published AS SELECT now() AS published_at, '{}'::JSON AS tables",
    """
    CREATE TABLE marts.mart_detection_scorecard AS
    SELECT * FROM (VALUES
        ('teleport', 'spark', 'teleport', 10, 9, 0, 0.9, 5, 5, 1.0),
        ('teleport', 'dbt', 'teleport', 10, 10, 0, 1.0, 5, 5, 1.0),
        ('teleport', 'any', 'all checks', NULL, NULL, NULL, NULL, 5, 5, 1.0),
        ('orphan_trip', 'dbt', 'orphan_trip', 6, 3, 2, 0.5, 4, 3, 0.75),
        ('orphan_trip', 'any', 'all checks', NULL, NULL, NULL, NULL, 4, 3, 0.75),
        ('schema_drift', 'bridge', 'contract_violation', 4, 4, 0, 1.0, 4, 4, 1.0),
        ('schema_drift', 'any', 'all checks', NULL, NULL, NULL, NULL, 4, 4, 1.0)
    ) t(fault_type, detector, check_name, detections, true_detections,
        explained_by_other_faults, precision, faults_judged, faults_found, recall)""",
    """
    CREATE TABLE marts.mart_data_quality_daily AS
    SELECT * FROM (VALUES
        (DATE '2026-03-02', 13000, 50, 0, 40, 60.5, 20, 5, 3, 4, 10, 1, 90, 2, 110),
        (DATE '2026-03-03', 16000, 70, 2, 55, 45.0, 25, 6, 4, 5, 12, 0, 120, 1, 130)
    ) t(day, events, duplicated_by_source, resent_by_bridge, late_events, max_lateness_min,
        dead_letters, dbt_over_capacity, dbt_teleports, dbt_orphan_trips, dbt_stale_snapshots,
        dbt_reporting_gaps, spark_alerts, spark_station_alerts, faults_injected)""",
    """
    CREATE TABLE marts.fct_trips AS
    SELECT * FROM (VALUES
        ('TR-1', 'member', 'electric', TIMESTAMPTZ '2026-03-02 08:10:00+00', 600, false,
         DATE '2026-03-02'),
        ('TR-2', 'casual', 'mechanical', TIMESTAMPTZ '2026-03-03 17:40:00+00', 900, true,
         DATE '2026-03-03')
    ) t(trip_id, rider_type, bike_type, started_at, duration_s, is_implausible_speed, trip_date)""",
    """
    CREATE TABLE marts.dim_stations AS
    SELECT * FROM (VALUES
        ('ST-001', 'Central Station', 'transit', 48.85, 2.35, 40),
        ('ST-002', 'Old Market', 'residential', 48.86, 2.36, 22)
    ) t(station_id, name, zone, lat, lon, capacity)""",
    """
    CREATE TABLE marts.mart_station_usage_hourly AS
    SELECT * FROM (VALUES
        ('ST-001', TIMESTAMPTZ '2026-03-02 08:00:00+00', 5, 2, 0, 10),
        ('ST-002', TIMESTAMPTZ '2026-03-03 17:00:00+00', 1, 6, 20, 0)
    ) t(station_id, hour, departures, arrivals, minutes_empty, minutes_full)""",
    """
    CREATE TABLE marts.mart_pipeline_health AS
    SELECT * FROM (VALUES
        (TIMESTAMPTZ '2026-09-16 20:00:00+00', 12, 30000::HUGEINT, 150.0, 290.0, 301.0),
        (TIMESTAMPTZ '2026-09-16 21:00:00+00', 12, 31000::HUGEINT, 148.0, 288.0, 300.0)
    ) t(landed_hour, batches, rows_landed, latency_p50_s, latency_p95_s, latency_max_s)""",
    """
    CREATE TABLE marts.mart_stream_vs_batch AS
    SELECT * FROM (VALUES
        (DATE '2026-03-02', 1500, 4000::HUGEINT, 4000::HUGEINT, 0::HUGEINT, 0),
        (DATE '2026-03-03', 1920, 4800::HUGEINT, 4791::HUGEINT, 9::HUGEINT, 9)
    ) t(day, windows, batch_trip_events, stream_trip_events, missing_in_stream,
        windows_that_differ)""",
]


def build_copy(path: Path, *extra: str) -> None:
    with duckdb.connect(str(path)) as conn:
        for statement in [*SERVING_COPY, *extra]:
            conn.execute(statement)


def run_app(serving: Path, monkeypatch) -> AppTest:
    monkeypatch.setenv("SERVING_PATH", str(serving))
    app = AppTest.from_file(APP, default_timeout=60)
    app.run()
    assert not app.exception, [e.value for e in app.exception]
    return app


def test_every_tab_renders_from_the_serving_copy(tmp_path, monkeypatch):
    serving = tmp_path / "bikeshare_serving.duckdb"
    build_copy(serving)

    app = run_app(serving, monkeypatch)

    assert [tab.label for tab in app.tabs] == ["Detection", "Data quality", "City", "Pipeline"]
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Faults judged"] == "13"  # the 'any' rows: 5 + 4 + 4
    assert metrics["Found by at least one check"] == "92.3%"  # 12 of 13
    assert metrics["Checks"] == "4"
    assert metrics["Events"] == "29,000"
    assert metrics["Completed trips"] == "2"
    assert metrics["Days where Spark and batch agree"] == "1 of 2"
    assert len(app.dataframe) >= 5  # a table view per tab, plus the days that differ


def test_a_new_published_copy_is_read_on_the_next_run(tmp_path, monkeypatch):
    """publish_serving swaps the file with os.replace: the cache must not serve the old one."""
    serving = tmp_path / "bikeshare_serving.duckdb"
    build_copy(serving)
    app = run_app(serving, monkeypatch)
    assert {m.label: m.value for m in app.metric}["Completed trips"] == "2"

    new = tmp_path / "bikeshare_serving.duckdb.tmp"
    one_more_trip = (
        "INSERT INTO marts.fct_trips "
        "SELECT * REPLACE ('TR-3' AS trip_id) FROM marts.fct_trips LIMIT 1"
    )
    build_copy(new, one_more_trip)
    os.utime(new, (serving.stat().st_mtime + 5,) * 2)  # a distinct mtime, even on a coarse clock
    os.replace(new, serving)
    app.run()

    assert {m.label: m.value for m in app.metric}["Completed trips"] == "3"


def test_without_a_serving_copy_it_says_how_to_get_one(tmp_path, monkeypatch):
    app = run_app(tmp_path / "missing.duckdb", monkeypatch)
    assert "No serving copy yet" in app.info[0].value
    assert not app.tabs
