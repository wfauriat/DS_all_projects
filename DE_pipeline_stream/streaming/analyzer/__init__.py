"""analyzer: the Spark Structured Streaming job that watches the bike-share stream.

    Kafka topics ──► main.py (four streaming queries) ──► alerts topic + Parquet lake

    config.py     settings from environment variables
    schemas.py    Spark schema of the events (mirrors the source contract)
    rules.py      the detection logic: pure DataFrame → DataFrame functions,
                  the same code for streaming and for batch (hence unit-testable)
    reference.py  station reference data (capacity, coordinates) from the source API
    progress.py   a listener that logs each micro-batch's progress as a JSON line
    main.py       builds the queries and starts them

This code runs inside the Spark image, which has Python 3.10: no 3.11+ features
here (streaming/ruff.toml checks that).
"""
