/*
======= Query Info =======                     
-- query_id: 3826778                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.991064                     
-- owner: hdser                     
==========================
*/

WITH

crc_new_users AS (
    SELECT 
        evt_block_date AS block_day
        ,COUNT(*) AS cnt
    FROM circles_ubi_gnosis.Hub_evt_Signup
    GROUP BY 1
),

calendar AS (
    SELECT
        block_day
    FROM (
        SELECT 
            MIN(block_day) AS block_day_min
        FROM 
            crc_new_users
    ),
    UNNEST(SEQUENCE(block_day_min,CURRENT_DATE, INTERVAL '1' DAY)) AS s(block_day)
    
)


SELECT
    t2.block_day
    ,COALESCE(t1.cnt,0) AS frozen_cnt
    ,SUM(COALESCE(t1.cnt,0)) OVER (ORDER BY t2.block_day) AS total_frozen
    ,COALESCE(t3.cnt,0) AS new_cnt
    ,SUM(COALESCE(t3.cnt,0)) OVER (ORDER BY t2.block_day) AS total
FROM (
    SELECT 
        block_day + INTERVAL '90' DAY AS frozen_date
        ,COUNT(*) AS cnt
    FROM 
        query_3827079 --gnosis_circles_frozen_users_v
    GROUP BY 1
) t1
RIGHT JOIN 
    calendar t2
    ON
    t2.block_day = t1.frozen_date
LEFT JOIN
    crc_new_users t3
    ON
    t3.block_day = t2.block_day