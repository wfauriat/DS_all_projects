-- Data quality per simulated day: how much arrived, how much of it was wrong, and who
-- noticed. One column per issue, so a day reads across and an issue reads down.
with events as (
    select event_time::date as day, copies, source_copies, lateness_min from {{ ref('stg_trip_events') }}
    union all
    select event_time::date, copies, source_copies, lateness_min from {{ ref('stg_station_status') }}
),

arrived as (
    select
        day,
        count(*)                                                        as events,
        count(*) filter (where source_copies > 1)                       as duplicated_by_source,
        count(*) filter (where copies > source_copies)                  as resent_by_bridge,
        count(*) filter (where lateness_min >= {{ var('late_after_min') }}) as late_events,
        round(max(lateness_min), 1)                                     as max_lateness_min
    from events
    group by day
),

findings as (
    select
        event_time::date as day,
        count(*) filter (where detector = 'bridge')                                         as dead_letters,
        count(*) filter (where detector = 'dbt'   and check_name = 'over_capacity')         as dbt_over_capacity,
        count(*) filter (where detector = 'dbt'   and check_name = 'teleport')              as dbt_teleports,
        count(*) filter (where detector = 'dbt'   and check_name = 'orphan_trip')           as dbt_orphan_trips,
        count(*) filter (where detector = 'dbt'   and check_name = 'stale_snapshot')        as dbt_stale_snapshots,
        count(*) filter (where detector = 'dbt'   and check_name = 'reporting_gap')         as dbt_reporting_gaps,
        count(*) filter (where detector = 'spark')                                          as spark_alerts,
        count(*) filter (where detector = 'spark' and check_name in ('frozen_station', 'silent_station')) as spark_station_alerts
    from {{ ref('int_detections') }}
    where event_time is not null
    group by all
),

injected as (
    select injected_at::date as day, count(*) as faults_injected
    from {{ ref('stg_fault_log') }}
    group by all
)

select
    a.*,
    coalesce(f.dead_letters, 0)         as dead_letters,
    coalesce(f.dbt_over_capacity, 0)    as dbt_over_capacity,
    coalesce(f.dbt_teleports, 0)        as dbt_teleports,
    coalesce(f.dbt_orphan_trips, 0)     as dbt_orphan_trips,
    coalesce(f.dbt_stale_snapshots, 0)  as dbt_stale_snapshots,
    coalesce(f.dbt_reporting_gaps, 0)   as dbt_reporting_gaps,
    coalesce(f.spark_alerts, 0)         as spark_alerts,
    coalesce(f.spark_station_alerts, 0) as spark_station_alerts,
    coalesce(i.faults_injected, 0)      as faults_injected
from arrived a
left join findings f using (day)
left join injected i using (day)
order by day
