"""Entry point: `python -m bikeshare_bridge`.

This is the Docker CMD (ingest/Dockerfile) and what `make bridge-run` runs on the host.
It reads the settings from the environment (see config.py), checks that Kafka is
reachable and the topics exist, then streams until it is stopped.
"""

import logging
import signal
import sys
from types import FrameType

import httpx
from confluent_kafka import KafkaException

from .bridge import Bridge
from .checkpoint import Checkpoint
from .config import load_settings
from .logs import event, setup
from .sink import KafkaSink

log = logging.getLogger("bridge")


def _stop(signum: int, frame: FrameType | None) -> None:
    # Turn SIGTERM (docker stop) into SystemExit. It unwinds whatever is running,
    # even a blocking socket read, straight into the `finally` below.
    raise SystemExit(0)


def main() -> None:
    settings = load_settings()
    setup(settings.log_level)
    signal.signal(signal.SIGTERM, _stop)

    sink = KafkaSink(settings.kafka_bootstrap)
    try:
        partitions = sink.check_topics(settings.topics.all())
    except (KafkaException, RuntimeError) as exc:  # unreachable broker, or missing topics
        event(log, "kafka not ready", level=logging.ERROR, error=str(exc))
        sys.exit(1)  # compose restarts us (restart: unless-stopped)

    # read=45 s: the source sends a keepalive every 15 s, so three missed ones
    # mean the connection is dead, even if TCP has not noticed yet.
    http = httpx.Client(timeout=httpx.Timeout(connect=5, read=45, write=5, pool=5))
    checkpoint = Checkpoint(settings.state_dir / "checkpoint.json")
    bridge = Bridge(settings, sink, checkpoint, http)
    event(log, "starting", source=settings.source_url, kafka=settings.kafka_bootstrap,
          partitions=partitions, checkpoint=bridge.last_seq, start_from=settings.start_from)  # fmt: skip
    try:
        bridge.run()
    except (SystemExit, KeyboardInterrupt):
        pass
    finally:
        bridge.close()
        http.close()


if __name__ == "__main__":
    main()
