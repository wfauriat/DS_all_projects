#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# kafka/create-topics.sh: every Kafka topic of the pipeline, declared in one place.
#
# Run by the one-shot service `kafka-init` (docker-compose.yml) once Kafka is
# healthy. `--if-not-exists` makes it safe to re-run on every `make up`.
#
# The broker runs with auto.create.topics.enable=false. A topic that is not
# declared here does not exist, and producing to it fails loudly, instead of
# silently creating a mistyped topic with default settings.
#
# Naming      <domain>.<dataset>.v<major version>. A breaking payload change
#             means a new topic (…v2) that consumers migrate to, not a silent edit.
# Partitions  the unit of parallelism and of ordering. Messages with the same
#             key always land in the same partition, so they stay in order.
#             Three partitions = up to three consumers of one group in parallel.
# Retention   how long Kafka keeps messages, in REAL time (not simulated time).
#             Anything still retained can be replayed, e.g. by a new consumer
#             group reading from the beginning.
#
# Topic names are also configured in the clients:
#   bridge (producer)    ingest/bikeshare_bridge/config.py
#   Spark (layer 3), Airflow loader (layer 4)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
DAY_MS=$((24 * 3600 * 1000))

create() {  # create <name> <partitions> <retention days>
  local name=$1 partitions=$2 days=$3
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config retention.ms=$((days * DAY_MS))
  echo "  ok  $name  (partitions=$partitions, retention=${days}d)"
}

echo "creating topics on $BOOTSTRAP"

# trip_started + trip_ended, keyed by bike_id: all the events of one bike stay in order.
create bikeshare.trip-events.v1 3 7

# station_status snapshots, keyed by station_id: per-station order.
# (cleanup.policy=compact would keep only the latest status per station. That
#  is handy for "current state" lookups, but it would drop the history we analyse.)
create bikeshare.station-status.v1 3 7

# Dead-letter queue: events the bridge rejected (contract violations), with the reason.
# One partition (low volume) and kept longer, because someone has to look at them.
create bikeshare.dlq.v1 1 30

# Alerts raised by the Spark analyzer (layer 3), keyed by the entity at fault.
create bikeshare.alerts.v1 3 7

echo "topics now on the broker:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
