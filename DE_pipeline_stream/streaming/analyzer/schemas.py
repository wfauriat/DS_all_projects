"""Spark schemas: the event contract as Spark sees it, plus the alert record.

Kafka hands Spark raw bytes. from_json() needs an explicit schema, and this is
it. The envelope mirrors source/bikeshare_sim/schemas.py. The payload is one
flat SUPERSET of every event type's fields: a field an event does not have
comes out as null, so all three types share one row layout.

Parsing never fails. A field that does not match comes out null, which is why
contract enforcement happens earlier, in the bridge. By the time events reach
these topics they have passed the contract.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PAYLOAD_SCHEMA = StructType(
    [
        # trip_started / trip_ended
        StructField("trip_id", StringType()),
        StructField("bike_id", StringType()),
        StructField("bike_type", StringType()),
        StructField("rider_type", StringType()),
        StructField("start_station_id", StringType()),
        StructField("duration_s", LongType()),
        # all three types (departure, arrival, or reporting station)
        StructField("station_id", StringType()),
        # station_status
        StructField("bikes_available", IntegerType()),
        StructField("ebikes_available", IntegerType()),
        StructField("docks_available", IntegerType()),
        StructField("is_renting", BooleanType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("seq", LongType()),
        StructField("event_time", TimestampType()),  # simulated: when it happened
        StructField("emitted_at", TimestampType()),  # simulated: when the source sent it
        StructField("payload", PAYLOAD_SCHEMA),
    ]
)

STATION_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("capacity", IntegerType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ]
)

# One row of bikeshare.alerts.v1 (serialized as JSON by rules.to_kafka_records).
ALERT_COLUMNS = [
    "alert_id",  # deterministic: the same finding always gets the same id (dedup downstream)
    "alert_type",
    "severity",
    "entity_type",  # station | trip | event
    "entity_id",
    "station_id",
    "event_id",  # the offending event, when there is one
    "event_time",
    "window_start",  # for window-based findings
    "window_end",
    "detail",  # JSON object with the rule's evidence
    "detector",  # which query found it
    "detected_at",  # wall clock (processing time)
]
