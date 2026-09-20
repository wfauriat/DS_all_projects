-- The source's ground truth: every fault it injected, with the keys needed to match
-- a detection to it (event_id, trip_id, or station + time span).
select
    fault_id,
    fault_type,
    injected_at,                                                    -- simulated time
    -- episodes last until ends_at; per-event faults are instants
    coalesce((details ->> '$.ends_at')::timestamptz, injected_at)   as ends_at,
    event_id,
    event_type,
    details ->> '$.station_id'                                      as station_id,
    details ->> '$.trip_id'                                         as trip_id,
    case fault_type
        when 'impossible_status' then
            case when (details ->> '$.bikes_available')::integer < 0 then 'negative' else 'over_capacity' end
        when 'orphan_trip' then 'dropped_' || (details ->> '$.dropped')
        when 'schema_drift' then details ->> '$.variant'
    end                                                             as variant,
    details::json                                                   as details
from {{ source('raw', 'fault_log') }}
