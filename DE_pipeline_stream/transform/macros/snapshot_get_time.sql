{#
    The clock dbt uses in snapshots, overridden for DuckDB.

    dbt-duckdb's version returns a naive TIMESTAMP, while every timestamp in
    this project is TIMESTAMPTZ (UTC). Returning the same type keeps snapshot
    comparisons like-for-like, and silences dbt's "data type … doesn't match"
    warning. dbt looks for dispatched macros in the root project first, so this
    one wins.
#}
{% macro duckdb__snapshot_get_time() -%}
    {{ current_timestamp() }}
{%- endmacro %}
