-- Station activity per simulated hour: departures, arrivals, occupancy, and the weather.
-- Built from the batch record (staging), so late events are counted where they belong.
with hours as (   -- every station × every hour with any data
    select s.station_id, h.hour
    from {{ ref('dim_stations') }} s
    cross join (
        select distinct date_trunc('hour', event_time) as hour from {{ ref('stg_station_status') }}
    ) h
),

trips as (
    select station_id, date_trunc('hour', event_time) as hour,
           count(*) filter (where event_type = 'trip_started') as departures,
           count(*) filter (where event_type = 'trip_ended')   as arrivals
    from {{ ref('stg_trip_events') }}
    group by all
),

occupancy as (
    select station_id, date_trunc('hour', event_time) as hour,
           round(avg(bikes_available), 1)                                                     as avg_bikes,
           count(*) filter (where bikes_available = 0) * {{ var('status_interval_min') }}    as minutes_empty,
           count(*) filter (where docks_available = 0) * {{ var('status_interval_min') }}    as minutes_full,
           count(*)                                                                           as status_reports
    from {{ ref('stg_station_status') }}
    group by all
)

select
    h.station_id,
    s.name          as station_name,
    s.zone,
    h.hour,
    dayname(h.hour) as weekday,
    coalesce(t.departures, 0)   as departures,
    coalesce(t.arrivals, 0)     as arrivals,
    o.avg_bikes,
    coalesce(o.minutes_empty, 0) as minutes_empty,
    coalesce(o.minutes_full, 0)  as minutes_full,
    coalesce(o.status_reports, 0) as status_reports,
    w.temperature_c,
    w.precipitation_mm
from hours h
join {{ ref('dim_stations') }} s using (station_id)
left join trips t using (station_id, hour)
left join occupancy o using (station_id, hour)
left join {{ ref('stg_weather') }} w on w.observed_at = h.hour
