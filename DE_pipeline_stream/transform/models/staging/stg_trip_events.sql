-- One row per trip event (trip_started, trip_ended), parsed from the JSON that Airflow landed.
--
-- Duplicates are removed here: the first copy landed wins. Two columns keep the evidence:
--   copies         how many times this event_id landed
--   source_copies  how many distinct source seqs carried it. A duplicate re-sent by the
--                  SOURCE got a new seq (the duplicate fault); a copy re-sent by the BRIDGE
--                  after a crash repeats the same seq (at-least-once delivery).
with parsed as (
    select
        value ->> '$.event_id'                          as event_id,
        value ->> '$.event_type'                        as event_type,
        (value ->> '$.seq')::bigint                     as source_seq,
        -- the three clocks
        (value ->> '$.event_time')::timestamptz         as event_time,   -- simulated: when it happened
        (value ->> '$.emitted_at')::timestamptz         as emitted_at,   -- simulated: when the source sent it
        msg_ts                                          as ingested_at,  -- wall: when the bridge produced it
        loaded_at,                                                       -- wall: when Airflow landed it
        value ->> '$.payload.trip_id'                   as trip_id,
        value ->> '$.payload.bike_id'                   as bike_id,
        value ->> '$.payload.bike_type'                 as bike_type,
        value ->> '$.payload.rider_type'                as rider_type,
        value ->> '$.payload.station_id'                as station_id,    -- departure or arrival station
        value ->> '$.payload.start_station_id'          as start_station_id,
        (value ->> '$.payload.duration_s')::integer     as duration_s,
        "partition",
        "offset"
    from {{ source('raw', 'trip_events') }}
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
