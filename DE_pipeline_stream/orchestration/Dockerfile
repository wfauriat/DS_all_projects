# ─────────────────────────────────────────────────────────────────────────────
# airflow image: Airflow 3.3 (Python 3.12) + the libraries the DAGs need.
#
# Built once and shared by every Airflow service in docker-compose.yml
# (x-airflow-common). DAGs and include/ are NOT baked in: they are mounted, so
# editing a DAG needs no rebuild. The dag-processor picks up changes within
# ~30 s.
# ─────────────────────────────────────────────────────────────────────────────
FROM apache/airflow:3.3.1-python3.12

# 1) Libraries for include/landing, installed into Airflow's own environment,
#    with apache-airflow pinned in the same command. That is the documented way
#    to extend the image: the resolver can then never swap Airflow's version.
#    duckdb: the SAME version as the host venv (uv.lock). The warehouse file is
#    written here and read on your machine, and both must share a storage format.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      confluent-kafka==2.15.1 \
      duckdb==1.5.5

# 2) dbt, in its OWN virtualenv (used from layer 5). dbt and Airflow pin
#    conflicting versions of shared libraries; a separate venv keeps both
#    intact. DAGs call it by path: /opt/dbt-venv/bin/dbt.
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir \
      dbt-core==1.12.5 dbt-duckdb==1.11.0 duckdb==1.5.5
USER airflow
