-- Alerts raised by the Spark analyzer (bikeshare.alerts.v1).
-- Spark's Kafka sink is at-least-once: a replayed micro-batch writes identical
-- alerts, with the same deterministic alert_id, so the first copy is kept.
select
    value ->> '$.alert_id'                          as alert_id,
    value ->> '$.alert_type'                        as alert_type,
    value ->> '$.severity'                          as severity,
    value ->> '$.entity_type'                       as entity_type,
    value ->> '$.entity_id'                         as entity_id,
    value ->> '$.station_id'                        as station_id,
    value ->> '$.event_id'                          as event_id,
    (value ->> '$.event_time')::timestamptz         as event_time,
    (value ->> '$.window_start')::timestamptz       as window_start,
    (value ->> '$.window_end')::timestamptz         as window_end,
    (value ->> '$.detail')::json                    as detail,
    value ->> '$.detector'                          as detector,
    (value ->> '$.detected_at')::timestamptz        as detected_at,  -- wall clock (Spark processing time)
    loaded_at
from {{ source('raw', 'alerts') }}
qualify row_number() over (partition by alert_id order by loaded_at, "partition", "offset") = 1
