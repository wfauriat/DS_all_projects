-- One row per station_status snapshot, parsed and deduplicated like stg_trip_events.
with parsed as (
    select
        value ->> '$.event_id'                              as event_id,
        (value ->> '$.seq')::bigint                         as source_seq,
        (value ->> '$.event_time')::timestamptz             as event_time,
        (value ->> '$.emitted_at')::timestamptz             as emitted_at,
        msg_ts                                              as ingested_at,
        loaded_at,
        value ->> '$.payload.station_id'                    as station_id,
        (value ->> '$.payload.bikes_available')::integer    as bikes_available,
        (value ->> '$.payload.ebikes_available')::integer   as ebikes_available,
        (value ->> '$.payload.docks_available')::integer    as docks_available,
        (value ->> '$.payload.is_renting')::boolean         as is_renting,
        "partition",
        "offset"
    from {{ source('raw', 'station_status') }}
),

copies as (
    select event_id, count(*) as copies, count(distinct source_seq) as source_copies
    from parsed
    group by event_id
)

select
    parsed.* exclude ("partition", "offset"),
    copies.copies,
    copies.source_copies,
    round((epoch(emitted_at) - epoch(event_time)) / 60, 1) as lateness_min
from parsed
join copies using (event_id)
qualify row_number() over (partition by event_id order by loaded_at, "partition", "offset") = 1
