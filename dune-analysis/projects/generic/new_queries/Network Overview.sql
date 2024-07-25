/*
======= Query Info =======                     
-- query_id: 3536272                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.223618                     
-- owner: hdser                     
==========================
*/

WITH 

hourly_addresses AS (
    SELECT
        DATE_TRUNC('hour', block_time) AS hour,
        "from"
    FROM gnosis.transactions
    GROUP BY 1, 2
),

first_appearance AS (
    SELECT
        "from",
        MIN(hour) AS first_hour
    FROM hourly_addresses
    GROUP BY "from"
), 

hourly_returning_addresses AS (
    SELECT
        w.hour,
        COUNT(DISTINCT CASE WHEN w.hour > f.first_hour THEN w."from" END) AS returning_addresses
    FROM 
        hourly_addresses w
    JOIN first_appearance f 
        ON w."from" = f."from"
    GROUP BY w.hour
), 


total_addresses_per_hour AS (
    SELECT
        hour,
        COUNT(DISTINCT "from") AS total_addresses
    FROM hourly_addresses
    GROUP BY hour
),


daily_returning_addresses AS (
    SELECT
         DATE_TRUNC('day', w.hour) As day,
         MAX(w.hour) AS hour,
        COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', w.hour) > DATE_TRUNC('day',f.first_hour)  THEN w."from" END) AS returning_addresses
    FROM 
        hourly_addresses w
    JOIN first_appearance f 
        ON w."from" = f."from"
    GROUP BY 1
), 

total_addresses_per_day AS (
    SELECT
        DATE_TRUNC('day', hour) As day,
        --MAX(hour) AS hour,
        COUNT(DISTINCT "from") AS total_addresses
    FROM hourly_addresses
    GROUP BY 1
),

users_hourly AS (
SELECT
    t.hour AS hour,
    COALESCE(w.returning_addresses, 0) AS returning,
    t.total_addresses - COALESCE(w.returning_addresses, 0) AS new
FROM total_addresses_per_hour t
LEFT JOIN hourly_returning_addresses w 
    ON t.hour = w.hour
WHERE
    t.hour >= DATE_TRUNC('day', NOW()) - INTERVAL '24' HOUR
    AND
    t.hour < DATE_TRUNC('day', NOW())
),

users_daily AS (
SELECT
    *
    ,SUM(new) OVER (ORDER BY day) AS cumsum_new
FROM (
    SELECT
        t.day,
       -- t.hour AS hour,
        COALESCE(w.returning_addresses, 0) AS returning,
        t.total_addresses - COALESCE(w.returning_addresses, 0) AS new
    FROM total_addresses_per_day t
    LEFT JOIN daily_returning_addresses w 
        ON t.day = w.day
   -- WHERE
   --     t.day > NOW() - INTERVAL '3' YEAR
    )
),

users_daily_rate AS (
    SELECT
        *
        ,(CAST(cumsum_new AS REAL)/(NULLIF(LAG(cumsum_new,7) OVER (ORDER BY day),0)) - 1)*100 AS pct_change_7d
        ,(CAST(cumsum_new AS REAL)/(NULLIF(LAG(cumsum_new,30) OVER (ORDER BY day),0)) - 1)*100 AS pct_change_30d
    FROM
        users_daily
),

daily_transactions AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      COUNT(CASE WHEN success = TRUE THEN 1 END) AS tx_success,
      COUNT(CASE WHEN success = FALSE THEN 1 END) AS tx_fail,
      COUNT(CASE WHEN success = TRUE THEN 1 END)/86400.0 AS tps
    FROM gnosis.transactions
   -- WHERE
    --    block_time > NOW() - INTERVAL '3' YEAR
    GROUP BY
      1
),

final_daily AS (
    SELECT
        t1.day,
        t1.returning,
        t1.new,
        t1.cumsum_new,
        t1.pct_change_7d,
        t1.pct_change_30d,
        t2.tx_success,
        t2.tx_fail,
        APPROX_PERCENTILE(t2.tps,0.5) OVER (ORDER BY t1.day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tps
    FROM
        users_daily_rate t1
    LEFT JOIN
        daily_transactions t2
        ON 
        t2.day = t1.day
    WHERE
        t1.day >= DATE_TRUNC('day', NOW())  - INTERVAL '3' YEAR
        AND
        t1.day < DATE_TRUNC('day', NOW())
),

last_day_txs_per_hour AS (
    SELECT
         date_trunc('hour', block_time) AS hour
        ,type AS tx_type
        ,COUNT(CASE WHEN success THEN hash END) AS cnt
        ,COUNT(CASE WHEN NOT success THEN hash END) AS cnt_failed
    FROM 
        gnosis.transactions
    WHERE
        block_time >= DATE_TRUNC('day', NOW())  - INTERVAL '24' HOUR
        AND
        block_time < DATE_TRUNC('day', NOW())
    GROUP BY
        date_trunc('hour', block_time)
        ,type
)

SELECT 
    day,
    NULL AS hour,
    returning,
    new,
    cumsum_new,
    pct_change_7d,
    pct_change_30d,
    tx_success,
    tx_fail,
    tps,
    NULL AS tx_type,
    NULL AS cnt,
    NULL AS cnt_failed
FROM
    final_daily
UNION ALL
SELECT
    NULL AS day,
    hour,
    returning,
    new,
    NULL AS cumsum_new,
    NULL AS pct_change_7d,
    NULL AS pct_change_30d,
    NULL AS tx_success,
    NULL AS tx_fail,
    NULL AS tps,
    NULL AS tx_type,
    NULL AS cnt,
    NULL AS cnt_failed
FROM
    users_hourly 
UNION ALL
    SELECT
        NULL AS day,
        hour,
        NULL AS returning,
        NULL AS new,
        NULL AS cumsum_new,
        NULL AS pct_change_7d,
        NULL AS pct_change_30d,
        NULL AS tx_success,
        NULL AS tx_fail,
        NULL AS tps,
        tx_type,
        cnt,
        cnt_failed
    FROM
        last_day_txs_per_hour
ORDER BY 1 DESC

