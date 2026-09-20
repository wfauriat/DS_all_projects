"""A local SparkSession for the rule tests (pyspark from the `spark` dependency group).

The rules are written against the DataFrame API, so they run unchanged on
small static DataFrames. No Kafka, no streaming, no container.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("analyzer-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
