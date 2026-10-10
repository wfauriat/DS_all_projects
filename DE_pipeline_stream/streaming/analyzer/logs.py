"""JSON log lines for the job's own messages. Spark's JVM logs go through log4j2
(streaming/conf/log4j2.properties) and are kept at WARN, so these stand out."""

import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup(level: str = "INFO") -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("analyzer")
    logger.handlers[:] = [handler]
    logger.setLevel(level)
    logger.propagate = False
    return logger


def event(logger: logging.Logger, msg: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, msg, extra={"fields": fields})
