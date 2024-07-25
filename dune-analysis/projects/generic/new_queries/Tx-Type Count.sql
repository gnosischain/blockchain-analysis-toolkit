/*
======= Query Info =======                     
-- query_id: 3514003                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.189936                     
-- owner: hdser                     
==========================
*/

WITH

last_day_txs_per_hour AS (
    SELECT
         date_trunc('hour', block_time) AS hour
        ,type AS tx_type
        ,COUNT(CASE WHEN success THEN hash END) AS cnt
        ,COUNT(CASE WHEN NOT success THEN hash END) AS cnt_failed
    FROM 
        gnosis.transactions
    WHERE
        block_time >= NOW() - INTERVAL '24' HOUR
    GROUP BY
        date_trunc('hour', block_time)
        ,type
),

cumulative_txs AS (
SELECT
    MAX(hour) AS hour
    ,SUM(cnt) AS cnt_tot
    ,SUM(cnt_failed) AS cnt_failed_tot
FROM
    last_day_txs_per_hour
)

SELECT
     t1.hour
    ,t1.tx_type
    ,t1.cnt
    ,t2.cnt_tot
    ,t2.cnt_failed_tot
FROM
    last_day_txs_per_hour t1
LEFT JOIN
    cumulative_txs t2
    ON 
    t2.hour = t1.hour
ORDER BY
    t1.hour DESC

