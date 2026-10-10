-- Completed trips faster than any bike (teleport fault). Expected to find rows: warn,
-- and keep them in `audit.trip_speed_implausible`.
{{ config(severity='warn', store_failures=true, schema='audit') }}

select trip_id, start_station_id, end_station_id, distance_km, duration_s, speed_kmh
from {{ ref('fct_trips') }}
where is_implausible_speed
