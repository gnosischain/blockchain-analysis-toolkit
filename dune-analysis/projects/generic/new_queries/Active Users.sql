/*
======= Query Info =======                     
-- query_id: 3510997                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.134421                     
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
        MAX(hour) AS hour,
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
    t.hour > NOW() - INTERVAL '24' HOUR
),

users_daily AS (
SELECT
    t.day,
    t.hour AS hour,
    COALESCE(w.returning_addresses, 0) AS returning,
    t.total_addresses - COALESCE(w.returning_addresses, 0) AS new
FROM total_addresses_per_day t
LEFT JOIN daily_returning_addresses w 
    ON t.day = w.day
WHERE
    t.day > NOW() - INTERVAL '3' YEAR
)

SELECT
    day,
    NULL AS hour,
    returning,
    new
FROM
    users_daily 
UNION ALL
SELECT
    NULL AS day,
    hour,
    returning,
    new
FROM
    users_hourly 
ORDER BY 1 DESC

