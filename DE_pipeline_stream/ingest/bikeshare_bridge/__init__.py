"""bikeshare_bridge: source SSE stream → Kafka (implemented in layer 2).

Its place in the flow: source-api (SSE) → [this bridge] → Kafka topics, read
by Spark and by the Airflow DuckDB loader.
"""
