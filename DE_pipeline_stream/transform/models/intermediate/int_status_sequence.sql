{{ config(materialized='table') }}
-- Every station_status snapshot, next to the previous snapshot of the same station and the
-- trips that touched that station in between. This sequential view gives the EXACT checks
-- that Spark, working on windows, can only approximate:
--
--   stale_snapshot  identical to the previous snapshot, although bikes came and went in
--                   between with a non-zero net flow. The count had to change, so the
--                   station is repeating itself (a frozen station).
--   reporting_gap   more than two reporting intervals since the previous snapshot (a silent
--                   station). Measured in event time, so late or stalled deliveries don't
--                   create false gaps.
--
-- A table, not a view: the range join below is the heaviest step of the build.
with status as (
    select
        station_id,
        event_id,
        event_time,
        bikes_available,
        docks_available,
        lag(event_time)      over by_station as prev_event_time,
        lag(bikes_available) over by_station as prev_bikes,
        lag(docks_available) over by_station as prev_docks
    from {{ ref('stg_station_status') }}
    window by_station as (partition by station_id order by event_time)
),

moves as (   -- +1 per bike docked at the station, -1 per bike taken
    select station_id, event_time, event_id, case event_type when 'trip_ended' then 1 else -1 end as delta
    from {{ ref('stg_trip_events') }}
),

between_snapshots as (
    select s.event_id, count(*) as trips_between, sum(m.delta) as net_flow,
           list(m.event_id) as trip_event_ids   -- kept so a finding can be traced to the trips behind it
    from status s
    join moves m
      on m.station_id = s.station_id
     and m.event_time >= s.prev_event_time
     and m.event_time <  s.event_time
    group by s.event_id
)

select
    s.*,
    coalesce(b.trips_between, 0)                                        as trips_between,
    coalesce(b.net_flow, 0)                                             as net_flow,
    b.trip_event_ids,
    round((epoch(s.event_time) - epoch(s.prev_event_time)) / 60, 1)     as gap_min,
    coalesce(s.bikes_available = s.prev_bikes
             and s.docks_available = s.prev_docks
             and b.net_flow <> 0, false)                                as stale_snapshot,
    coalesce(gap_min > 2 * {{ var('status_interval_min') }}, false)     as reporting_gap
from status s
left join between_snapshots b using (event_id)
