{#
    How many Parquet files Spark has written for a lake dataset.

    A view over read_parquet() fails to build if the glob matches no file: before
    Spark has closed its first window, or when the stack runs without the "stream"
    profile. Models reading the lake call this first and fall back to an empty
    result. At parse time (execute = false) nothing is queried.
#}
{% macro lake_file_count(dataset) %}
    {% if not execute %}
        {{ return(0) }}
    {% endif %}
    {% set pattern = env_var('LAKE_DIR', '../data/lake') ~ '/' ~ dataset ~ '/*/*.parquet' %}
    {% set result = run_query("select count(*) from glob('" ~ pattern ~ "')") %}
    {{ return(result.columns[0].values()[0]) }}
{% endmacro %}
