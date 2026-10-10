"""The streaming job: four queries, each one a Structured Streaming pattern.

    Kafka  bikeshare.trip-events.v1 + bikeshare.station-status.v1
      │   (each query has its own Kafka source and its own checkpoint)
      ├─► reference_rules        stateless, foreachBatch + refreshed station data
      │                          over_capacity · teleport · late_event → alerts topic
      ├─► station_metrics        watermark + dedup + 30-min windows → Parquet lake (file sink)
      ├─► station_health         watermark + dedup + 2-hour windows → frozen/silent_station → alerts
      └─► trip_pairing           stream-stream full outer join → orphan_trip → alerts topic

Why four queries rather than one: a query has one sink, and stateful and
stateless logic don't mix well. Late events are the clearest case:
reference_rules must see them to flag them, while the windowed queries must
ignore them past the watermark. Each query runs independently, keeps its
progress (Kafka offsets, state) in data/checkpoints/<query>/, and resumes
from there after a restart. It commits nothing to Kafka, so Kafka's consumer
groups (and the Console's lag view) don't show Spark.

Delivery: the Parquet file sink is exactly-once (its _spark_metadata log
records the committed files). The Kafka sink is at-least-once, so after a
crash a batch may be written twice; alerts carry deterministic ids for that.

Run by spark-submit (see streaming/Dockerfile → CMD), e.g. `make logs s=spark`.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import rules
from .config import Settings, load_settings
from .logs import event, setup
from .progress import ProgressLogger
from .reference import StationReference

log = logging.getLogger("analyzer")


def main() -> None:
    setup()
    cfg = load_settings()
    # master, memory, shuffle partitions, state store: streaming/conf/spark-defaults.conf
    spark = SparkSession.builder.getOrCreate()
    spark.streams.addListener(ProgressLogger())
    event(log, "starting", spark=spark.version, kafka=cfg.kafka_bootstrap, lake=cfg.lake_dir,
          watermark=cfg.watermark, metrics_window_min=cfg.station_window_min,
          health_window_min=cfg.health_window_min)  # fmt: skip

    start_reference_rules(spark, cfg)
    start_station_metrics(spark, cfg)
    start_station_health(spark, cfg)
    start_trip_pairing(spark, cfg)

    # Block until any query fails. The container exits, compose restarts it, and
    # every query resumes from its checkpoint.
    spark.streams.awaitAnyTermination()


# ── sources ─────────────────────────────────────────────────────────────────
def events_stream(spark: SparkSession, cfg: Settings) -> DataFrame:
    kafka_rows = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", f"{cfg.topics.trip_events},{cfg.topics.station_status}")
        # Only used the very first time (no checkpoint yet). Afterwards the checkpoint decides.
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        # If retention deleted offsets we never read (job down for days), carry on with a warning.
        .option("failOnDataLoss", "false")
        .load()
    )
    return rules.parse_events(kafka_rows)


def deduplicated(events: DataFrame, cfg: Settings) -> DataFrame:
    """Drop repeats of an event_id (source duplicates, bridge re-sends) seen within the watermark."""
    return events.withWatermark("event_time", cfg.watermark).dropDuplicatesWithinWatermark(
        ["event_id"]
    )


def _checkpoint(cfg: Settings, query: str) -> str:
    return f"{cfg.checkpoint_dir}/{query}"


def _trigger(cfg: Settings) -> dict:
    return {"processingTime": f"{cfg.trigger_seconds} seconds"}


def _to_alerts_topic(alerts: DataFrame, cfg: Settings, query: str):
    return (
        rules.to_kafka_records(alerts)
        .writeStream.queryName(query)
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("topic", cfg.topics.alerts)
        .option("checkpointLocation", _checkpoint(cfg, query))
        .outputMode("append")
        .trigger(**_trigger(cfg))
        .start()
    )


# ── the four queries ────────────────────────────────────────────────────────
def start_reference_rules(spark: SparkSession, cfg: Settings) -> None:
    """Stateless rules that need station reference data. No watermark here, so late events count."""
    reference = StationReference(cfg.source_url)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        # Runs on the driver once per micro-batch, with `batch` as a plain DataFrame.
        # persist(): three rules read `batch`. Without it, each would re-read the
        # micro-batch from Kafka (visible as 3× input rows in the progress log).
        batch.persist()
        try:
            stations = reference.dataframe(batch.sparkSession)
            alerts = (
                rules.over_capacity(batch, stations)
                .unionByName(rules.teleports(batch, stations))
                .unionByName(rules.late_events(batch, cfg.late_after_min))
            )
            (
                rules.to_kafka_records(alerts)
                .write.format("kafka")
                .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
                .option("topic", cfg.topics.alerts)
                .save()
            )
        finally:
            batch.unpersist()

    (
        events_stream(spark, cfg)
        .writeStream.queryName("reference_rules")
        .foreachBatch(write_batch)
        .option("checkpointLocation", _checkpoint(cfg, "reference_rules"))
        .trigger(**_trigger(cfg))
        .start()
    )


def station_windows(spark: SparkSession, cfg: Settings, minutes: int) -> DataFrame:
    events = deduplicated(events_stream(spark, cfg), cfg)
    return rules.station_windows(events, f"{minutes} minutes")


def start_station_metrics(spark: SparkSession, cfg: Settings) -> None:
    """Windowed metrics to the lake. A window is written once, when the watermark closes it."""
    (
        station_windows(spark, cfg, cfg.station_window_min)
        .withColumn("date", F.to_date("window_start"))
        .writeStream.queryName("station_metrics")
        .format("parquet")
        .option("path", f"{cfg.lake_dir}/station_metrics")
        .option("checkpointLocation", _checkpoint(cfg, "station_metrics"))
        .partitionBy("date")  # data/lake/station_metrics/date=2026-03-02/part-….parquet
        .outputMode("append")
        .trigger(**_trigger(cfg))
        .start()
    )


def start_station_health(spark: SparkSession, cfg: Settings) -> None:
    """Longer windows than the metrics: frozen/silent need hours of evidence to be told
    apart from a quiet station (see rules.window_alerts). An alert therefore
    arrives up to one window plus the watermark after the fault started."""
    windows = station_windows(spark, cfg, cfg.health_window_min)
    expected = cfg.health_window_min // cfg.status_interval_min
    _to_alerts_topic(rules.window_alerts(windows, expected), cfg, "station_health")


def start_trip_pairing(spark: SparkSession, cfg: Settings) -> None:
    """No dedup here: a duplicated start or end just matches the same partner again."""
    pairs = rules.trip_pairs(events_stream(spark, cfg), cfg.watermark, cfg.max_trip_duration)
    _to_alerts_topic(rules.orphan_alerts(pairs), cfg, "trip_pairing")
