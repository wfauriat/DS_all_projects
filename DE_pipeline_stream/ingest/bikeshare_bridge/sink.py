"""Kafka side of the bridge: an idempotent producer with delivery tracking.

librdkafka (under confluent-kafka) batches and sends in a background thread.
produce() only enqueues a message. The broker's acknowledgement arrives later,
through the delivery callback, which is served by poll() and flush(). So the
bridge moves its checkpoint only after flush() confirms that every message sent
so far is in Kafka (see bridge.py).
"""

import logging
from collections import Counter
from collections.abc import Callable

from confluent_kafka import KafkaError, Message, Producer

from .contract import Routed

PRODUCER_CONFIG = {
    # Retries never create duplicates or reorder messages within a partition.
    # This also implies acks=all (wait for every in-sync replica).
    "enable.idempotence": True,
    "compression.type": "zstd",
    "linger.ms": 20,  # wait up to 20 ms to fill a batch: fewer, bigger requests
    "delivery.timeout.ms": 60_000,  # give up on a message (and report it) after 60 s
    "client.id": "bikeshare-bridge",  # shows up in broker logs and metrics
}


class DeliveryFailed(Exception):
    """Kafka did not acknowledge some messages: the checkpoint must not move past them."""


class KafkaSink:
    def __init__(self, bootstrap: str, producer_factory: Callable[[dict], Producer] = Producer):
        config = {
            "bootstrap.servers": bootstrap,
            # librdkafka's own messages go through Python logging (and so come out
            # as JSON), instead of raw "%4|…" lines on stderr.
            "logger": logging.getLogger("librdkafka"),
            **PRODUCER_CONFIG,
        }
        self._producer = producer_factory(config)
        self.delivered: Counter[str] = Counter()  # topic → acknowledged messages
        self._failures: list[str] = []

    def check_topics(self, names: list[str], timeout: float = 15.0) -> dict[str, int]:
        """Fail fast if Kafka is unreachable or a topic is missing (kafka-init not run?)."""
        metadata = self._producer.list_topics(timeout=timeout)
        missing = [n for n in names if n not in metadata.topics]
        if missing:
            raise RuntimeError(f"missing Kafka topics {missing}: see kafka/create-topics.sh")
        return {n: len(metadata.topics[n].partitions) for n in names}

    def send(self, message: Routed) -> None:
        while True:
            try:
                self._producer.produce(
                    message.topic,
                    key=message.key,
                    value=message.value,
                    headers=message.headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # The local queue is full (Kafka slow or down). Serve callbacks, then retry.
                # This backpressure propagates up to the SSE read, and so to the source.
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve delivery callbacks of earlier messages

    def flush(self, timeout: float) -> None:
        """Block until everything sent so far is acknowledged, or raise DeliveryFailed."""
        remaining = self._producer.flush(timeout)
        if self._failures:
            failures, self._failures = self._failures, []
            raise DeliveryFailed(f"{len(failures)} message(s) not delivered, e.g. {failures[0]}")
        if remaining:
            raise DeliveryFailed(f"{remaining} message(s) still unacknowledged after {timeout}s")

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self._failures.append(f"{msg.topic()}: {err}")
        else:
            self.delivered[msg.topic()] += 1
