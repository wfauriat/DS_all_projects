-- Stations as they are now, one row each: the current version from the SCD2 snapshot,
-- plus how many versions (capacity changes) each station has been through.
with versions as (
    select station_id, count(*) as versions, min(capacity) as first_capacity
    from {{ ref('snap_stations') }}
    group by station_id
)

select
    s.station_id,
    s.name,
    s.zone,
    s.lat,
    s.lon,
    s.capacity,
    v.first_capacity,
    v.versions,
    s.installed_at,
    s.dbt_valid_from as current_since
from {{ ref('snap_stations') }} s
join versions v using (station_id)
where s.dbt_valid_to = '9999-12-31 00:00:00+00'::timestamptz
