{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert',
    )
}}
-- Completed trips: one row per trip, with duration, distance and speed.
--
-- INCREMENTAL: after the first build, each run only (re)processes trips whose latest
-- half LANDED since the last run. Filtering on landing time (loaded_at), not on
-- event_time, is what makes late events safe: a trip_ended delayed by three
-- simulated hours still has a fresh loaded_at, so its trip is picked up and
-- upserted (delete+insert on trip_id).
select
    trip_id,
    bike_id,
    bike_type,
    rider_type,
    start_station_id,
    end_station_id,
    started_at,
    ended_at,
    duration_s,
    distance_km,
    speed_kmh,
    speed_kmh > {{ var('teleport_min_kmh') }}   as is_implausible_speed,
    started_at::date                            as trip_date,
    loaded_at
from {{ ref('int_trips') }}
where pairing = 'complete'
{% if is_incremental() %}
  and loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
