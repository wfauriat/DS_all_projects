import sqlite3


def query_daily_volume(conn: sqlite3.Connection) \
    -> list[tuple[str, str, int, int]]:
    QUERY_DAILY_VOLUME = """
    SELECT strftime('%Y-%m-%d', occurred_at) AS day, channel, 
    COUNT(*) AS count,
    SUM(order_total_cents) AS total_cents
    FROM orders_current
    GROUP BY day, channel
    ORDER BY day ASC, channel DESC;
    """
    cursor = conn.execute(QUERY_DAILY_VOLUME)
    result = []
    for row in cursor.fetchall():
        date, channel, count, total_cents = row
        result.append((date, channel, count, total_cents))
    return result


def query_weekday_weekend_split(conn: sqlite3.Connection) -> \
    list[tuple[str, int, int, float]]:
    QUERY_WEEKDAY_WEEKEND_SPLIT = """
    WITH per_dow AS (
        SELECT strftime('%w', occurred_at) AS dow, 
            COUNT(*) AS n,
            COUNT(DISTINCT strftime('%Y-%m-%d', occurred_at)) AS n_days
        FROM orders_current GROUP BY dow )
    SELECT CASE WHEN dow IN ('0', '6') THEN 'weekend' 
        ELSE 'weekday' END AS bucket,
        SUM(n) AS total_orders,
        SUM(n_days) AS total_days,
        ROUND(SUM(n) * 1.0 / SUM(n_days), 2) AS avg_orders_per_day
    FROM per_dow GROUP BY bucket;    
    """
    cursor = conn.execute(QUERY_WEEKDAY_WEEKEND_SPLIT)
    result = []
    for row in cursor.fetchall():
        bucket, total_orders, total_days, avg_orders_per_day = row
        result.append((bucket, total_orders,
                       total_days, avg_orders_per_day))
    return result

def query_orders_as_of(conn: sqlite3.Connection, as_of: str) -> list:
    QUERY_ORDERS_AS_OF = """
    WITH ranked AS (
        SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY order_id ORDER BY version DESC,
                knowledge_time DESC) AS rn
        FROM stg_orders
        WHERE knowledge_time <= ?
    )
    SELECT order_id, customer_id, occurred_at, 
           channel, payment_method, shipping_speed, 
           order_total_cents, knowledge_time, content_hash, 
           version FROM ranked 
    WHERE rn = 1
    """
    cursor = conn.execute(QUERY_ORDERS_AS_OF, (as_of,))
    result = []
    for row in cursor.fetchall():
        result.append(row)
    return result

def query_segment_country(conn: sqlite3.Connection) -> tuple[list, int]:
    QUERY_SEGMENT_COUNTRY = """
    SELECT cc.segment, cc.country,
       COUNT(*) AS n_orders,
       SUM(oc.order_total_cents) AS total_cents
    FROM orders_current oc
    JOIN customers_current cc ON oc.customer_id = cc.customer_id
    GROUP BY cc.segment, cc.country
    ORDER BY total_cents DESC;
    """
    QUERY_TOTAL = """
    SELECT SUM(n_orders) FROM (
    SELECT cc.segment, cc.country, COUNT(*) AS n_orders
    FROM orders_current oc
    JOIN customers_current cc ON oc.customer_id = cc.customer_id
    GROUP BY cc.segment, cc.country
    );
    """
    cursor = conn.execute(QUERY_SEGMENT_COUNTRY)
    result = []
    for row in cursor.fetchall():
        result.append(row)
    cursor = conn.execute(QUERY_TOTAL)
    total = cursor.fetchone()
    return result, total