-- The pipeline watching itself, per wall-clock hour of landing:
--   landing   batches and rows landed by Airflow (raw._ingest_batches)
--   latency   how long events took from the bridge (Kafka record time) to DuckDB
-- Wall-clock time throughout: this is about the pipeline, not about the simulated city.
with batches as (
    select
        date_trunc('hour', finished_at)                                     as landed_hour,
        count(*)                                                            as batches,
        sum(rows)                                                           as rows_landed,
        count(*) filter (where source like 'kafka:%')                       as kafka_batches,
        count(*) filter (where source like 'api:%')                         as api_batches,
        round(avg(epoch(finished_at) - epoch(started_at)), 1)               as avg_batch_seconds
    from {{ source('raw', '_ingest_batches') }}
    group by all
),

latency as (
    select
        date_trunc('hour', loaded_at)                                       as landed_hour,
        round(quantile_cont(epoch(loaded_at) - epoch(ingested_at), 0.5))   as latency_p50_s,
        round(quantile_cont(epoch(loaded_at) - epoch(ingested_at), 0.95))  as latency_p95_s,
        round(max(epoch(loaded_at) - epoch(ingested_at)))                  as latency_max_s
    from (
        select ingested_at, loaded_at from {{ ref('stg_trip_events') }}
        union all
        select ingested_at, loaded_at from {{ ref('stg_station_status') }}
    )
    group by all
)

select *
from batches
left join latency using (landed_hour)
order by landed_hour
