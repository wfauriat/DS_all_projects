"""Station reference data (capacity, coordinates) for the stream-static joins.

It comes from the source's REST API (GET /v1/stations), the batch side of the
vendor. Capacities change over time (station expansions), so a DataFrame
loaded once at startup would go stale and raise false over_capacity alerts
after an expansion. The data is therefore re-fetched every `ttl_s` seconds, and
used inside foreachBatch, which runs on the driver once per micro-batch.
"""

import json
import logging
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession

from .logs import event
from .schemas import STATION_SCHEMA

log = logging.getLogger("analyzer")


class StationReference:
    def __init__(self, source_url: str, ttl_s: float = 60.0) -> None:
        self.url = f"{source_url}/v1/stations"
        self.ttl_s = ttl_s
        self._df: DataFrame | None = None
        self._fetched_at = 0.0

    def dataframe(self, spark: SparkSession) -> DataFrame:
        if self._df is None or time.monotonic() - self._fetched_at > self.ttl_s:
            try:
                self._df = spark.createDataFrame(self._fetch(), STATION_SCHEMA)
                self._fetched_at = time.monotonic()
            except OSError as exc:  # source down: keep the previous copy, if any
                if self._df is None:
                    raise
                event(log, "station reference refresh failed; using the previous copy",
                      level=logging.WARNING, error=str(exc))  # fmt: skip
        return self._df

    def _fetch(self) -> list[tuple]:
        with urllib.request.urlopen(self.url, timeout=10) as response:
            stations = json.load(response)
        return [(s["station_id"], s["capacity"], s["lat"], s["lon"]) for s in stations]
