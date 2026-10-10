"""The bridge's position in the source stream: the last seq known to be in Kafka.

It is saved only after the producer's flush() has confirmed delivery. After a
crash, the bridge therefore resumes at or before its true position: at-least-once.
Events sent twice this way are duplicates, which consumers drop by event_id.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path


class Checkpoint:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> int | None:
        """None means "never started": BRIDGE_START_FROM then decides where to begin."""
        if not self.path.exists():
            return None
        return int(json.loads(self.path.read_text())["last_seq"])

    def save(self, last_seq: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"last_seq": last_seq, "saved_at": datetime.now(UTC).isoformat()})
        )
        os.replace(tmp, self.path)  # atomic: never a half-written checkpoint

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)
