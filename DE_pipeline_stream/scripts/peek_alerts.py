"""Summarize the alerts topic from the host: counts by type, and the latest alerts.

    make alerts        (= uv run python scripts/peek_alerts.py)

A plain Kafka consumer on your machine, going through Kafka's EXTERNAL listener
(localhost:9094). It reads the whole topic with assign(): no consumer group
membership and no committed offsets, so it changes nothing for anyone else.

"distinct" counts unique alert_ids. Spark's Kafka sink is at-least-once, so a
replayed micro-batch can write the same alert twice. The deterministic
alert_id lets consumers drop the repeat.
"""

import json
import os
from collections import Counter

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

TOPIC = "bikeshare.alerts.v1"


def read_all(bootstrap: str) -> list[dict]:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": "peek-alerts", "enable.auto.commit": False}
    )
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    ends = {
        p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
        for p in partitions
    }
    consumer.assign([TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in partitions])
    alerts, done = [], {p for p, end in ends.items() if end == 0}
    while len(done) < len(partitions):
        msg = consumer.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        alerts.append(json.loads(msg.value()))
        if msg.offset() + 1 >= ends[msg.partition()]:
            done.add(msg.partition())
    consumer.close()
    return alerts


def main() -> None:
    alerts = read_all(f"localhost:{os.environ.get('KAFKA_HOST_PORT', '9094')}")
    if not alerts:
        print("no alerts yet: Spark raises them as faults occur (try `make fault f=teleport`)")
        return
    total = Counter(a["alert_type"] for a in alerts)
    distinct = Counter(a["alert_type"] for a in {a["alert_id"]: a for a in alerts}.values())
    print(f"{'alert_type':<16} {'alerts':>7} {'distinct':>9}   detector")
    for alert_type, n in total.most_common():
        detector = next(a["detector"] for a in alerts if a["alert_type"] == alert_type)
        print(f"{alert_type:<16} {n:>7} {distinct[alert_type]:>9}   {detector}")
    print("\nlatest:")
    for a in sorted(alerts, key=lambda a: a["detected_at"])[-5:]:
        when = a.get("event_time") or a.get("window_start")
        print(f"  {a['alert_type']:<15} {a['entity_id']:<14} at {when}  {a['detail']}")


if __name__ == "__main__":
    main()
