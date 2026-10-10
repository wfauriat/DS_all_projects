WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id 
    ORDER BY version DESC, knowledge_time DESC) AS rn 
    FROM {{ ref('stg_orders') }}
) 
SELECT order_id, customer_id, occurred_at, 
    channel, payment_method, shipping_speed, 
    order_total_cents, knowledge_time, content_hash, version 
FROM ranked WHERE rn = 1