/*
======= Query Info =======                     
-- query_id: 3509177                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.032173                     
-- owner: hdser                     
==========================
*/

WITH 
daily_addresses AS (
    SELECT
        DATE_TRUNC('week', block_time) AS day,
        "from"
    FROM gnosis.transactions
    GROUP BY 1, 2
),
first_appearance AS (
    SELECT
        "from",
        MIN(day) AS first_day
    FROM daily_addresses
    GROUP BY "from"
), 
daily_returning_addresses AS (
    SELECT
        w.day,
        COUNT(DISTINCT CASE WHEN w.day > f.first_day THEN w."from" END) AS returning_addresses
    FROM 
        daily_addresses w
    JOIN first_appearance f 
        ON w."from" = f."from"
    GROUP BY w.day
), 
total_addresses_per_day AS (
    SELECT
        day,
        COUNT(DISTINCT "from") AS total_addresses
    FROM daily_addresses
    GROUP BY day
)

SELECT 
    current_day
    ,new_addresses AS "New"
    ,SUM(new_addresses) OVER (ORDER BY current_day) AS "Total"
FROM (
    SELECT
        t.day AS current_day,
        t.total_addresses - COALESCE(w.returning_addresses, 0) AS new_addresses
    FROM total_addresses_per_day t
    LEFT JOIN daily_returning_addresses w 
        ON t.day = w.day
)