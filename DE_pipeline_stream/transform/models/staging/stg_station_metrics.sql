-- Spark's 30-minute per-station windows, read in place from the Parquet lake.
--
-- Referencing the source up front records the lineage even when the lake is
-- still empty. In that case (Spark has not closed a window yet, or the stack
-- runs without the "stream" profile) the model returns no rows instead of failing.
{%- set lake = source('lake', 'station_metrics') %}

{% if lake_file_count('station_metrics') > 0 %}
select
    window_start::timestamptz   as window_start,   -- Spark writes naive UTC timestamps
    window_end::timestamptz     as window_end,
    station_id,
    status_reports,
    min_bikes,
    max_bikes,
    avg_bikes,
    departures,
    arrivals
from {{ lake }}
{% else %}
select
    null::timestamptz as window_start, null::timestamptz as window_end, null::varchar as station_id,
    null::bigint as status_reports, null::integer as min_bikes, null::integer as max_bikes,
    null::double as avg_bikes, null::bigint as departures, null::bigint as arrivals
where false
{% endif %}
