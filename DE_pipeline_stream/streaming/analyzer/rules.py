"""Detection rules: pure DataFrame → DataFrame functions.

Structured Streaming uses the same DataFrame API for streams and for tables.
Every function here works on a streaming DataFrame (main.py) and on a small
static one (streaming/tests), which is what makes the rules unit-testable.

    parse_events        Kafka rows → one typed row per event
    over_capacity       status × station reference   → over_capacity alerts
    teleports           trip_ended × station coords  → teleport alerts
    late_events         emitted_at − event_time      → late_event alerts
    station_windows     per-station activity per window (a stateful aggregation)
    window_alerts       windows → frozen_station / silent_station alerts
    trip_pairs          trip_started ⟗ trip_ended (a stateful stream-stream join)
    orphan_alerts       unmatched pairs → orphan_trip alerts
    to_kafka_records    alerts → key/value rows for the Kafka sink

What needs STATE (watermarks, windows, joins) and what doesn't is the main
design axis. It decides which query each rule runs in; see main.py.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schemas import ALERT_COLUMNS, EVENT_SCHEMA

TELEPORT_MIN_SPEED_KMH = 45.0
EARTH_RADIUS_KM = 6371.0


# ── parsing ─────────────────────────────────────────────────────────────────
def parse_events(kafka_rows: DataFrame) -> DataFrame:
    """Kafka rows (key, value, topic, partition, offset, timestamp, …) → flat typed events.

    Kafka's record `timestamp` is set by the bridge when it produced the message:
    that is `ingested_at`, the pipeline's wall clock. The other two clocks,
    event_time and emitted_at (both simulated), come from inside the event.
    """
    event = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA)
    return kafka_rows.select(
        event.alias("e"),
        F.col("timestamp").alias("ingested_at"),
        "topic",
        "partition",
        "offset",
    ).select(
        "e.event_id",
        "e.event_type",
        "e.seq",
        "e.event_time",
        "e.emitted_at",
        "e.payload.*",
        "ingested_at",
        "topic",
        "partition",
        "offset",
    )


# ── alert shape ─────────────────────────────────────────────────────────────
def _col(value, spark_type: str) -> Column:
    """A Column as-is, or a typed literal (typed nulls keep unions compatible)."""
    return value if isinstance(value, Column) else F.lit(value).cast(spark_type)


def make_alerts(
    rows: DataFrame,
    *,
    alert_type,
    entity_type: str,
    entity_id: Column,
    detector: str,
    detail: dict,
    severity: str = "warning",
    station_id=None,
    event_id=None,
    event_time=None,
    window_start=None,
    window_end=None,
) -> DataFrame:
    """Shape any rule's output as rows of bikeshare.alerts.v1 (see schemas.ALERT_COLUMNS)."""
    alerts = rows.select(
        _col(alert_type, "string").alias("alert_type"),
        F.lit(severity).alias("severity"),
        F.lit(entity_type).alias("entity_type"),
        entity_id.cast("string").alias("entity_id"),
        _col(station_id, "string").alias("station_id"),
        _col(event_id, "string").alias("event_id"),
        _col(event_time, "timestamp").alias("event_time"),
        _col(window_start, "timestamp").alias("window_start"),
        _col(window_end, "timestamp").alias("window_end"),
        F.to_json(F.struct(*[v.alias(k) for k, v in detail.items()])).alias("detail"),
        F.lit(detector).alias("detector"),
        F.current_timestamp().alias("detected_at"),
    )
    # Deterministic id: a replayed micro-batch produces identical alerts, and
    # consumers drop repeats by alert_id (the Kafka sink is at-least-once).
    identity = F.concat_ws(
        "|",
        "alert_type",
        "entity_id",
        F.coalesce(F.col("event_id"), F.col("window_start").cast("string")),
    )
    return alerts.withColumn("alert_id", F.substring(F.sha2(identity, 256), 1, 16)).select(
        ALERT_COLUMNS
    )


def to_kafka_records(alerts: DataFrame) -> DataFrame:
    """The Kafka sink wants `key` and `value` columns. The key (entity) picks the partition.

    ignoreNullFields=false: every alert carries every field, with explicit nulls.
    Consumers get one stable schema instead of keys that come and go.
    """
    return alerts.select(
        F.col("entity_id").alias("key"),
        F.to_json(F.struct(*ALERT_COLUMNS), {"ignoreNullFields": "false"}).alias("value"),
    )


# ── stateless rules (need reference data, no state) ─────────────────────────
def over_capacity(events: DataFrame, stations: DataFrame) -> DataFrame:
    """More bikes than the station has docks. The contract can't see that: it needs the capacity."""
    status = events.where(F.col("event_type") == "station_status")
    joined = status.join(F.broadcast(stations.select("station_id", "capacity")), "station_id")
    hits = joined.where(F.col("bikes_available") > F.col("capacity"))
    return make_alerts(
        hits,
        alert_type="over_capacity",
        entity_type="station",
        entity_id=F.col("station_id"),
        station_id=F.col("station_id"),
        event_id=F.col("event_id"),
        event_time=F.col("event_time"),
        detail={"bikes_available": F.col("bikes_available"), "capacity": F.col("capacity")},
        detector="spark.reference_rules",
    )


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(
        F.sin(dlon / 2), 2
    )
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def teleports(
    events: DataFrame, stations: DataFrame, min_speed_kmh: float = TELEPORT_MIN_SPEED_KMH
) -> DataFrame:
    """A trip whose straight-line distance ÷ duration beats any bike: the end station is wrong."""
    ends = events.where((F.col("event_type") == "trip_ended") & (F.col("duration_s") > 0))
    start = F.broadcast(stations.select(
        F.col("station_id").alias("s_id"), F.col("lat").alias("s_lat"), F.col("lon").alias("s_lon")
    ))  # fmt: skip
    end = F.broadcast(stations.select(
        F.col("station_id").alias("e_id"), F.col("lat").alias("e_lat"), F.col("lon").alias("e_lon")
    ))  # fmt: skip
    trips = ends.join(start, F.col("start_station_id") == F.col("s_id")).join(
        end, F.col("station_id") == F.col("e_id")
    )
    km = haversine_km(F.col("s_lat"), F.col("s_lon"), F.col("e_lat"), F.col("e_lon"))
    trips = trips.withColumn("km", km).withColumn("kmh", F.col("km") / (F.col("duration_s") / 3600))
    hits = trips.where(F.col("kmh") > min_speed_kmh)
    return make_alerts(
        hits,
        alert_type="teleport",
        entity_type="trip",
        entity_id=F.col("trip_id"),
        station_id=F.col("station_id"),
        event_id=F.col("event_id"),
        event_time=F.col("event_time"),
        detail={
            "start_station_id": F.col("start_station_id"),
            "km": F.round("km", 2),
            "duration_s": F.col("duration_s"),
            "kmh": F.round("kmh", 1),
        },
        detector="spark.reference_rules",
    )


def late_events(events: DataFrame, late_after_min: float) -> DataFrame:
    """The source stamped the event long after it happened (late_event fault, or a stream stall)."""
    lateness_min = (F.col("emitted_at").cast("long") - F.col("event_time").cast("long")) / 60
    hits = events.withColumn("lateness_min", lateness_min).where(
        F.col("lateness_min") >= late_after_min
    )
    return make_alerts(
        hits,
        alert_type="late_event",
        severity="info",
        entity_type="event",
        entity_id=F.col("event_id"),
        station_id=F.col("station_id"),
        event_id=F.col("event_id"),
        event_time=F.col("event_time"),
        detail={"event_type": F.col("event_type"), "lateness_min": F.round("lateness_min", 1)},
        detector="spark.reference_rules",
    )


# ── stateful: per-station windows ───────────────────────────────────────────
def station_windows(events: DataFrame, window: str) -> DataFrame:
    """Per station and window: status reports, bike counts seen, departures, arrivals.

    Every event type carries payload.station_id (departure, arrival or reporting
    station), so one groupBy covers all three. In a stream the caller has set a
    watermark: a window is emitted once, when the watermark passes its end.
    """
    is_status = F.col("event_type") == "station_status"
    bikes = F.when(is_status, F.col("bikes_available"))
    docks = F.when(is_status, F.col("docks_available"))
    return (
        events.groupBy(F.window("event_time", window).alias("w"), "station_id")
        .agg(
            F.sum(is_status.cast("int")).alias("status_reports"),
            F.min(bikes).alias("min_bikes"),
            F.max(bikes).alias("max_bikes"),
            F.round(F.avg(bikes), 2).alias("avg_bikes"),
            F.min(docks).alias("min_docks"),
            F.max(docks).alias("max_docks"),
            F.sum((F.col("event_type") == "trip_started").cast("int")).alias("departures"),
            F.sum((F.col("event_type") == "trip_ended").cast("int")).alias("arrivals"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "station_id",
            "status_reports",
            "min_bikes",
            "max_bikes",
            "avg_bikes",
            "min_docks",
            "max_docks",
            "departures",
            "arrivals",
        )
    )


def window_alerts(windows: DataFrame, expected_reports: int, min_trips: int = 3) -> DataFrame:
    """Station health over a window of `expected_reports` status intervals.

    frozen  most reports arrived, all identical, although ≥ min_trips trips
            started or ended there
    silent  fewer than half the reports arrived, although trips went on there

    Both need trips. A quiet station at 3 a.m. legitimately repeats itself, and
    that is no evidence of anything. The window must be long enough: at 30 min,
    one departure plus one arrival, or trips after the last snapshot, make a
    healthy station look frozen. Measured against the ground truth, 30-minute
    windows gave ~8% precision and 2-hour windows ~70%. The exact
    snapshot-by-snapshot check is sequential, and belongs in batch SQL (dbt, layer 5).
    """
    trips = F.col("departures") + F.col("arrivals")
    frozen = (
        (F.col("status_reports") >= expected_reports * 2 / 3)
        & (F.col("min_bikes") == F.col("max_bikes"))
        & (F.col("min_docks") == F.col("max_docks"))
        & (trips >= min_trips)
    )
    # Strictly less than half: the stream's first window is often half-covered
    # (the world started mid-window), and that must not look like a silent station.
    silent = (F.col("status_reports") < expected_reports / 2) & (trips >= min_trips)
    hits = windows.where(frozen | silent)
    return make_alerts(
        hits,
        alert_type=F.when(frozen, "frozen_station").otherwise("silent_station"),
        entity_type="station",
        entity_id=F.col("station_id"),
        station_id=F.col("station_id"),
        window_start=F.col("window_start"),
        window_end=F.col("window_end"),
        detail={
            "status_reports": F.col("status_reports"),
            "bikes_seen": F.col("min_bikes"),
            "departures": F.col("departures"),
            "arrivals": F.col("arrivals"),
        },
        detector="spark.station_windows",
    )


# ── stateful: trip pairing ──────────────────────────────────────────────────
def trip_pairs(events: DataFrame, watermark: str, max_duration: str) -> DataFrame:
    """Full outer join of starts and ends on trip_id, within max_duration.

    In a stream both sides need a watermark and the join needs a time bound.
    Together they tell Spark when a start can no longer find its end, so its
    state can be dropped and the unmatched row emitted (with nulls on the
    missing side). withWatermark is a no-op on static DataFrames.
    """
    started = (
        events.where(F.col("event_type") == "trip_started")
        .select(
            F.col("trip_id").alias("s_trip_id"),
            F.col("event_time").alias("started_at"),
            F.col("station_id").alias("start_station_id"),
            F.col("event_id").alias("start_event_id"),
        )
        .withWatermark("started_at", watermark)
    )
    ended = (
        events.where(F.col("event_type") == "trip_ended")
        .select(
            F.col("trip_id").alias("e_trip_id"),
            F.col("event_time").alias("ended_at"),
            F.col("station_id").alias("end_station_id"),
            F.col("event_id").alias("end_event_id"),
        )
        .withWatermark("ended_at", watermark)
    )
    matches = (
        (F.col("s_trip_id") == F.col("e_trip_id"))
        & (F.col("ended_at") >= F.col("started_at"))
        & (F.col("ended_at") <= F.col("started_at") + F.expr(f"INTERVAL {max_duration}"))
    )
    return started.join(ended, matches, "full_outer")


def orphan_alerts(pairs: DataFrame) -> DataFrame:
    no_end = F.col("e_trip_id").isNull()
    orphans = pairs.where(no_end | F.col("s_trip_id").isNull())
    return make_alerts(
        orphans,
        alert_type="orphan_trip",
        entity_type="trip",
        entity_id=F.coalesce("s_trip_id", "e_trip_id"),
        station_id=F.when(no_end, F.col("start_station_id")).otherwise(F.col("end_station_id")),
        event_id=F.when(no_end, F.col("start_event_id")).otherwise(F.col("end_event_id")),
        event_time=F.when(no_end, F.col("started_at")).otherwise(F.col("ended_at")),
        detail={"missing": F.when(no_end, F.lit("trip_ended")).otherwise(F.lit("trip_started"))},
        detector="spark.trip_pairing",
    )
