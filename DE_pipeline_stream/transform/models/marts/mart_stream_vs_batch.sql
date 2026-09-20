-- Two computations of the same numbers: departures and arrivals per station per
-- 30-minute window. One comes from Spark in real time (the lake), one from the
-- batch record in DuckDB. They should agree. Where they don't, the difference has
-- a cause worth knowing:
--   missing_in_stream  events Spark dropped because they arrived behind its
--                      watermark (late events, stream stalls). The batch side
--                      landed them anyway.
-- Only windows that are final on BOTH sides are compared: closed by Spark, and
-- already landed by Airflow. Spark runs ahead of the 5-minute landing.
with compared_until as (
    select least(
        (select max(window_end) from {{ ref('stg_station_metrics') }}),
        (select time_bucket(interval '30 minutes', max(event_time)) from {{ ref('stg_trip_events') }})
    ) as t
),

stream as (
    select window_start, station_id, departures, arrivals
    from {{ ref('stg_station_metrics') }}
    where window_end <= (select t from compared_until)
),

batch as (
    select
        time_bucket(interval '30 minutes', event_time) as window_start,
        station_id,
        count(*) filter (where event_type = 'trip_started') as departures,
        count(*) filter (where event_type = 'trip_ended')   as arrivals
    from {{ ref('stg_trip_events') }}
    where event_time < (select t from compared_until)
    group by all
)

select
    coalesce(b.window_start, s.window_start)::date                  as day,
    count(*)                                                        as windows,
    sum(coalesce(b.departures, 0) + coalesce(b.arrivals, 0))        as batch_trip_events,
    sum(coalesce(s.departures, 0) + coalesce(s.arrivals, 0))        as stream_trip_events,
    batch_trip_events - stream_trip_events                          as missing_in_stream,
    count(*) filter (where coalesce(b.departures, 0) <> coalesce(s.departures, 0)
                        or coalesce(b.arrivals, 0)   <> coalesce(s.arrivals, 0)) as windows_that_differ
from batch b
full join stream s using (window_start, station_id)
group by all
order by day
