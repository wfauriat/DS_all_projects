-- How good is each check at the fault it targets, measured against the source's
-- ground truth (raw.fault_log)?
--
--   precision   share of the check's detections that point at a real fault of a targeted type
--   recall      share of the injected faults of that type (old enough to judge) that the
--               check found
--   explained   detections that match no fault of their own type but are caused by
--               ANOTHER fault: one fault showing up as another (e.g. an "orphan" trip
--               whose other half was dead-lettered because of schema drift)
--
-- Rows with detector = 'any' give each fault type's recall across all checks together.
--
-- Judged only up to a horizon where BOTH sides are complete. Events land every 5 minutes, but
-- the fault log is extracted every 15 (at 60× speed, 15 simulated hours). A detection newer than
-- the latest extracted fault would otherwise count as false, only because its fault is not known yet.
with checks as (
    select * from {{ ref('detection_checks') }}
),

detections as (
    select * from {{ ref('int_detections') }}
),

faults as (
    select * from {{ ref('stg_fault_log') }}
),

judged_until as (
    select least(
        -- events: a fault younger than this may legitimately still be undetected (windows, runs)
        (select max(event_time) from {{ ref('stg_trip_events') }}) - interval '{{ var("judge_after_hours") }} hours',
        -- ground truth: the fault log is complete up to its latest extracted fault
        (select max(injected_at) from faults)
    ) as t
),

matches as (   -- every (detection, fault) pair the checks table allows
    select distinct d.detection_id, d.detector, d.check_name, f.fault_id, f.fault_type
    from detections d
    join checks c
      on c.detector = d.detector and c.check_name = d.check_name
    join faults f
      on f.fault_type = c.fault_type
     and (
            (c.match_on = 'event'   and f.event_id = d.event_id)
         or (c.match_on = 'trip'    and f.trip_id = d.trip_id)
         or (c.match_on = 'episode' and f.station_id = d.station_id
                                    and d.window_start < f.ends_at and d.window_end > f.injected_at)
         or (c.match_on = 'stall'   and d.event_time >= f.injected_at and d.event_time < f.ends_at)
     )
),

explained as (   -- "false" detections that another fault caused
    -- an orphan trip whose other half was rejected by the bridge (schema drift)
    select d.detection_id
    from detections d
    join {{ ref('stg_dead_letters') }} dl on dl.trip_id = d.trip_id
    where d.check_name = 'orphan_trip'
    union
    -- a "stale" snapshot that counted a bike which never came: a teleported trip_ended
    -- reported this station instead of the real one
    select d.detection_id
    from detections d
    join {{ ref('int_status_sequence') }} q on q.event_id = d.event_id
    join faults f on f.fault_type = 'teleport' and list_contains(q.trip_event_ids, f.event_id)
    where d.check_name = 'stale_snapshot'
),

precision_by_check as (
    select
        d.detector,
        d.check_name,
        count(*)                                                        as detections,
        count(*) filter (where m.detection_id is not null)              as true_detections,
        count(*) filter (where m.detection_id is null and e.detection_id is not null) as explained_by_other_faults
    from detections d
    left join (select distinct detection_id from matches) m using (detection_id)
    left join explained e using (detection_id)
    where coalesce(d.window_start, d.event_time) < (select t from judged_until)
    group by all
),

judged_faults as (
    select f.* from faults f, judged_until j where f.injected_at < j.t
),

recall_by_check as (   -- every check of the seed, even before it has anything to judge
    select
        c.detector,
        c.check_name,
        c.fault_type,
        count(distinct f.fault_id)  as faults_judged,
        count(distinct m.fault_id)  as faults_found
    from checks c
    left join judged_faults f on f.fault_type = c.fault_type
    left join matches m
      on m.fault_id = f.fault_id and m.detector = c.detector and m.check_name = c.check_name
    group by all
),

recall_any as (
    select
        'any' as detector,
        'all checks' as check_name,
        t.fault_type,
        count(distinct f.fault_id)  as faults_judged,
        count(distinct m.fault_id)  as faults_found
    from (select distinct fault_type from checks) t
    left join judged_faults f on f.fault_type = t.fault_type
    left join matches m on m.fault_id = f.fault_id
    group by all
)

select
    r.fault_type::varchar                                               as fault_type,
    r.detector::varchar                                                 as detector,
    r.check_name::varchar                                               as check_name,
    -- 'any' rows carry recall only: their detection columns stay NULL rather than a misleading 0
    case when r.detector <> 'any' then coalesce(p.detections, 0) end::bigint                as detections,
    case when r.detector <> 'any' then coalesce(p.true_detections, 0) end::bigint           as true_detections,
    case when r.detector <> 'any' then coalesce(p.explained_by_other_faults, 0) end::bigint as explained_by_other_faults,
    round(p.true_detections / nullif(p.detections, 0), 3)::double      as precision,
    r.faults_judged::bigint                                             as faults_judged,
    r.faults_found::bigint                                              as faults_found,
    round(r.faults_found / nullif(r.faults_judged, 0), 3)::double      as recall
from (select * from recall_by_check union all select * from recall_any) r
left join precision_by_check p
  on p.detector = r.detector and p.check_name = r.check_name
order by r.fault_type, r.detector = 'any', r.detector, r.check_name
