"""Forget what the warehouse landed from a deleted Kafka topic: its rows and positions, together.

    make spark-reset   (runs it after deleting the alerts topic:
                        PYTHONPATH=orchestration/include uv run python scripts/forget_landed_topic.py bikeshare.alerts.v1)

The loader resumes each partition from the offset stored in DuckDB. Those
offsets number the messages of the DELETED topic, and the topic that kafka-init
creates again starts over at 0. Kept, they made the loader skip every replayed
alert below them (measured on 2026-09-16: 85 alerts never reached the warehouse).
landing.kafka_loader.forget_topic explains the rest.

It acts only once Kafka confirms the topic is gone, because topic deletion is
asynchronous: a landing run that still saw the old topic would land it again from
offset 0. If Kafka can't be reached, the topic was not deleted either, and the
stored positions are still right.
"""

import os
import sys
import time
from pathlib import Path

import duckdb
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from landing import kafka_loader, warehouse

WAREHOUSE = Path("data/warehouse/bikeshare.duckdb")
BOOTSTRAP = f"localhost:{os.environ.get('KAFKA_HOST_PORT', '9094')}"


def topic_exists(admin: AdminClient, topic: str) -> bool:
    return topic in admin.list_topics(timeout=10).topics


def main() -> None:
    topic = sys.argv[1]
    if not WAREHOUSE.exists():
        print(f"{WAREHOUSE} does not exist: nothing landed from {topic}")
        return

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        for _ in range(30):
            if not topic_exists(admin, topic):
                break
            time.sleep(1)
        else:
            sys.exit(f"{topic} still exists after 30 s: warehouse left as it is")
    except KafkaException:
        print(f"Kafka is not reachable: {topic} was not deleted, its stored positions still hold")
        return

    with warehouse.connect(str(WAREHOUSE)) as conn:
        try:
            forgotten = kafka_loader.forget_topic(conn, topic)
        except duckdb.CatalogException:  # no raw schema yet: the landing DAG never ran
            print(f"the warehouse has no raw tables yet: nothing landed from {topic}")
            return
    print(
        f"forgot {forgotten['rows']:,} rows and {forgotten['positions']} stored positions of {topic}: "
        "the next stream_landing run lands the new topic from offset 0"
    )


if __name__ == "__main__":
    main()
