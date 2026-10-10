-- Trips: each trip_started paired with its trip_ended, by trip_id.
-- The batch twin of Spark's trip_pairing query (a stream-stream join).
--
-- pairing:
--   complete       both halves arrived
--   in_progress    no end yet, but started less than max_trip_hours before the latest event
--   missing_end    no end, and too old to still be riding (an orphan)
--   missing_start  an end without its start (an orphan)
-- The "latest event" plays the role of Spark's watermark: without it, every
-- ride under way would look like an orphan.
with starts as (
    select trip_id, event_id as start_event_id, event_time as started_at, station_id as start_station_id,
           bike_id, bike_type, rider_type, loaded_at
    from {{ ref('stg_trip_events') }}
    where event_type = 'trip_started'
),

ends as (
    select trip_id, event_id as end_event_id, event_time as ended_at, station_id as end_station_id,
           start_station_id as reported_start_station_id, duration_s, bike_id, bike_type, loaded_at
    from {{ ref('stg_trip_events') }}
    where event_type = 'trip_ended'
),

latest as (
    select max(event_time) as latest_event_time from {{ ref('stg_trip_events') }}
),

stations as (
    select station_id, lat, lon from {{ ref('stg_stations') }}   -- station coordinates never change
),

paired as (
    select
        coalesce(s.trip_id, e.trip_id)                              as trip_id,
        coalesce(s.bike_id, e.bike_id)                              as bike_id,
        coalesce(s.bike_type, e.bike_type)                          as bike_type,
        s.rider_type,
        s.start_event_id,
        e.end_event_id,
        coalesce(s.start_station_id, e.reported_start_station_id)   as start_station_id,
        e.end_station_id,
        s.started_at,
        e.ended_at,
        e.duration_s,
        case
            when s.trip_id is not null and e.trip_id is not null then 'complete'
            when e.trip_id is null
                 and s.started_at > l.latest_event_time - interval '{{ var("max_trip_hours") }} hours'
                then 'in_progress'
            when e.trip_id is null then 'missing_end'
            else 'missing_start'
        end                                                         as pairing,
        -- when the trip's latest half landed: drives fct_trips' incremental loads
        coalesce(greatest(s.loaded_at, e.loaded_at), s.loaded_at, e.loaded_at) as loaded_at
    from starts s
    full join ends e on s.trip_id = e.trip_id
    cross join latest l
)

select
    paired.*,
    round({{ haversine_km('a.lat', 'a.lon', 'b.lat', 'b.lon') }}, 3)   as distance_km,
    round(distance_km / nullif(duration_s, 0) * 3600, 1)              as speed_kmh
from paired
left join stations a on a.station_id = paired.start_station_id
left join stations b on b.station_id = paired.end_station_id
