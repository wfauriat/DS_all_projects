"""serving: what the pipeline hands to its readers (analysts, notebooks, dashboards).

    publish.py   copies the marts into a separate DuckDB file, replaced atomically

Plain Python with no Airflow import, like `landing`; unit-tested in orchestration/tests.
"""
