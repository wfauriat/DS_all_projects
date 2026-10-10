-- Stations as last extracted from the API (the snapshot replaced at each api_extract run).
-- Their history of changes lives in snapshots/snap_stations.yml, built from this model.
select
    station_id,
    name,
    zone,
    lat,
    lon,
    capacity,
    installed_at,
    updated_at,     -- simulated time of the last change (a capacity expansion)
    extracted_at
from {{ source('raw', 'stations') }}
