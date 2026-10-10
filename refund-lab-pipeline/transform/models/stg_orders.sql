SELECT order_id, customer_id, occurred_at,
channel, payment_method, 
shipping_speed, order_total_cents, knowledge_time, 
content_hash, CAST(version AS INTEGER) AS version 
FROM {{ source('landing', 'orders_raw') }}