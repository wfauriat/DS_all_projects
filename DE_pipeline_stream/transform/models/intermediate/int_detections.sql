-- Every finding of every detector, in one shape, ready to be scored against the ground truth.
--
--   detector    bridge (contract check at ingestion) · spark (streaming rules) · dbt (batch checks below)
--   check_name  what found it; seeds/detection_checks.csv says which fault each check targets
--   keys        event_id / trip_id / station_id + [window_start, window_end]: whatever
--               the check can point at. The scorecard matches on the relevant key.
with bridge as (
    -- distinct: a drifted event the source ALSO duplicated is rejected twice, but it is one finding
    select distinct 'bridge' as detector, error_type as check_name,
           event_id, trip_id, station_id, event_time,
           event_time as window_start, event_time as window_end
    from {{ ref('stg_dead_letters') }}
),

spark as (
    select 'spark', alert_type,
           event_id, case when entity_type = 'trip' then entity_id end, station_id, event_time,
           coalesce(window_start, event_time), coalesce(window_end, event_time)
    from {{ ref('stg_alerts') }}
),

events as (   -- both event streams, for the checks that apply to any event
    select event_id, trip_id, station_id, event_time, source_copies, lateness_min
    from {{ ref('stg_trip_events') }}
    union all
    select event_id, null, station_id, event_time, source_copies, lateness_min
    from {{ ref('stg_station_status') }}
),

dbt_duplicate as (   -- the SOURCE sent the same event under several seqs (bridge re-sends excluded)
    select 'dbt', 'duplicate_event', event_id, trip_id, station_id, event_time, event_time, event_time
    from events where source_copies > 1
),

dbt_late as (
    select 'dbt', 'late_event', event_id, trip_id, station_id, event_time, event_time, event_time
    from events where lateness_min >= {{ var('late_after_min') }}
),

dbt_over_capacity as (   -- judged against the capacity VALID AT event_time (the SCD2 snapshot)
    select 'dbt', 'over_capacity', s.event_id, null, s.station_id, s.event_time, s.event_time, s.event_time
    from {{ ref('stg_station_status') }} s
    join {{ ref('snap_stations') }} v
      on v.station_id = s.station_id
     and s.event_time >= v.dbt_valid_from
     and s.event_time <  v.dbt_valid_to
    where s.bikes_available > v.capacity
),

dbt_teleport as (
    select 'dbt', 'teleport', end_event_id, trip_id, end_station_id, ended_at, ended_at, ended_at
    from {{ ref('int_trips') }}
    where speed_kmh > {{ var('teleport_min_kmh') }}
),

dbt_orphan as (
    select 'dbt', 'orphan_trip', coalesce(start_event_id, end_event_id), trip_id,
           coalesce(start_station_id, end_station_id), coalesce(started_at, ended_at),
           coalesce(started_at, ended_at), coalesce(started_at, ended_at)
    from {{ ref('int_trips') }}
    where pairing in ('missing_end', 'missing_start')
),

dbt_stale as (   -- the window is the span since the previous (identical) snapshot
    select 'dbt', 'stale_snapshot', event_id, null, station_id, event_time, prev_event_time, event_time
    from {{ ref('int_status_sequence') }} where stale_snapshot
),

dbt_gap as (
    select 'dbt', 'reporting_gap', event_id, null, station_id, event_time, prev_event_time, event_time
    from {{ ref('int_status_sequence') }} where reporting_gap
),

all_detections as (
    select * from bridge
    union all select * from spark
    union all select * from dbt_duplicate
    union all select * from dbt_late
    union all select * from dbt_over_capacity
    union all select * from dbt_teleport
    union all select * from dbt_orphan
    union all select * from dbt_stale
    union all select * from dbt_gap
)

select
    md5(concat_ws('|', detector, check_name, event_id, trip_id, station_id, window_start)) as detection_id,
    *
from all_detections
