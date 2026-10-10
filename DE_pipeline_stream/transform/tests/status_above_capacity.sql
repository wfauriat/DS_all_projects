-- A SINGULAR test: a query written for one situation. It returns the offending rows.
--
-- Station snapshots reporting more bikes than the station had docks at that time (as-of
-- join on the SCD2 snapshot). The source injects exactly this fault, so this test is
-- EXPECTED to find rows. At severity `warn` it reports without failing the build, and
-- with store_failures the rows are kept in `audit.status_above_capacity`, to query.
{{ config(severity='warn', store_failures=true, schema='audit') }}

select s.station_id, s.event_id, s.event_time, s.bikes_available, v.capacity
from {{ ref('stg_station_status') }} s
join {{ ref('snap_stations') }} v
  on v.station_id = s.station_id
 and s.event_time >= v.dbt_valid_from
 and s.event_time <  v.dbt_valid_to
where s.bikes_available > v.capacity
