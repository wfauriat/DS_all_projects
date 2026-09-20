"""A StreamingQueryListener that logs each micro-batch as one JSON line.

This is the job's "what did I just consume" log, the same idea as the bridge's
throughput line. The Spark UI's Structured Streaming tab (http://localhost:4040)
shows the same numbers as charts.

    input_rows          rows read from Kafka in this batch
    rows_per_s          processing rate
    batch_ms            wall time of the batch
    watermark           event-time watermark after the batch (simulated time)
    state_rows          rows held in the state store (windows, join buffers, dedup keys)
    dropped_late        rows that arrived behind the watermark and were ignored by stateful operators
    dropped_duplicates  rows removed by dropDuplicatesWithinWatermark
"""

import json
import logging

from pyspark.sql.streaming import StreamingQueryListener

from .logs import event

log = logging.getLogger("analyzer")


class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, e) -> None:
        event(log, "query started", query=e.name, id=str(e.id))

    def onQueryProgress(self, e) -> None:
        p = json.loads(e.progress.json)
        if not p.get("numInputRows"):
            return  # idle batch: nothing worth a line
        operators = p.get("stateOperators", [])
        event(
            log,
            "progress",
            query=p.get("name"),
            batch=p.get("batchId"),
            input_rows=p.get("numInputRows"),
            rows_per_s=round(p.get("processedRowsPerSecond") or 0, 1),
            batch_ms=p.get("durationMs", {}).get("triggerExecution"),
            watermark=p.get("eventTime", {}).get("watermark"),
            state_rows=sum(op.get("numRowsTotal", 0) for op in operators),
            dropped_late=sum(op.get("numRowsDroppedByWatermark", 0) for op in operators),
            dropped_duplicates=sum(
                op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for op in operators
            ),
        )

    def onQueryIdle(self, e) -> None:
        pass

    def onQueryTerminated(self, e) -> None:
        level = logging.ERROR if e.exception else logging.INFO
        event(log, "query terminated", level=level, id=str(e.id), error=e.exception)
