"""Structured logs: one JSON object per line on stdout.

Easy to read with `make logs s=bridge | jq`, and to ship to a log store. Pass
fields with extra={"fields": {...}}; the `event()` helper does that for you:

    event(log, "throughput", received=450, dlq=1)
    → {"ts": "...", "level": "INFO", "logger": "bridge", "msg": "throughput", "received": 450, "dlq": 1}
"""

import json
import logging
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise here


def event(logger: logging.Logger, msg: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, msg, extra={"fields": fields})
