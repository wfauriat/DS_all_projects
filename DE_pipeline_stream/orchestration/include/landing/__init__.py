"""landing: the code behind the Airflow DAGs that fill the DuckDB warehouse's `raw` schema.

Plain Python with no Airflow import, so it is unit-tested without Airflow
(orchestration/tests). The DAGs (orchestration/dags/) only schedule these
functions and hand them their connections.

    warehouse.py     the DuckDB file: connecting (single writer!) and the raw schema
    kafka_loader.py  Kafka topics → raw.<topic table>, in bounded batches, exactly once
    api_extract.py   the source's REST API → raw.stations / bikes / weather / fault_log

Inside the Airflow containers this folder is mounted at /opt/airflow/include,
which is on PYTHONPATH (see docker-compose.yml).
"""
