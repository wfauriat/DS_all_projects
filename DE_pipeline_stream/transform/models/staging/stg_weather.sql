-- Hourly weather observations (simulated time), appended incrementally by api_extract.
select
    observed_at,
    temperature_c,
    precipitation_mm,
    wind_kmh,
    precipitation_mm > 0 as is_raining
from {{ source('raw', 'weather') }}
