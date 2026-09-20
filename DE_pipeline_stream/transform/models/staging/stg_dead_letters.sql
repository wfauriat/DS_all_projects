-- Events the bridge rejected (bikeshare.dlq.v1), with what can still be read from them.
--
-- `raw` holds the original event text. Once parsed as JSON it usually still yields
-- the event_id, trip and station, which is enough to trace the rejection back to
-- the fault that caused it.
with parsed as (
    select
        value ->> '$.error_type'                        as error_type,
        value -> '$.errors'                             as errors,       -- JSON array of messages
        (value ->> '$.source_seq')::bigint              as source_seq,
        try_cast(value ->> '$.raw' as json)             as raw_event,    -- null if not even JSON
        msg_ts                                          as ingested_at,
        loaded_at
    from {{ source('raw', 'dlq') }}
)

select
    source_seq,
    error_type,
    errors,
    raw_event ->> '$.event_id'                          as event_id,
    raw_event ->> '$.event_type'                        as event_type,
    try_cast(raw_event ->> '$.event_time' as timestamptz) as event_time,
    raw_event ->> '$.payload.trip_id'                   as trip_id,
    raw_event ->> '$.payload.station_id'                as station_id,
    raw_event,
    ingested_at,
    loaded_at
from parsed
-- the bridge re-sends after a crash: same seq, same rejection
qualify row_number() over (partition by source_seq order by loaded_at) = 1
