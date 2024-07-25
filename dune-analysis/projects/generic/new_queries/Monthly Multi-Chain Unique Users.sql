/*
======= Query Info =======                     
-- query_id: 3913266                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=from_date, value=2024-01-01 00:00:00, type=datetime), Parameter(name=to_date, value=2024-07-12 00:00:00, type=datetime)]                     
-- last update: 2024-07-25 17:22:48.588530                     
-- owner: hdser                     
==========================
*/

WITH

txs AS (
SELECT DISTINCT
    DATE_TRUNC('month', t1.block_time) AS month
    ,t1.blockchain 
    ,t1."from" AS tx_from
FROM evms.transactions t1
WHERE
    t1.block_time >= TIMESTAMP '{{from_date}}'
    AND
    t1.block_time <= TIMESTAMP '{{to_date}}'
)


SELECT
*
FROM (
SELECT
    month
    ,blockchain_list
    ,cnt
    ,CAST(cnt AS REAL)/(SUM(cnt) OVER (PARTITION BY month)) AS pct
FROM (
SELECT
    month
    ,blockchain_list
    ,COUNT(*) AS cnt
FROM (
    SELECT
        month
        ,tx_from
        ,ARRAY_AGG(blockchain ORDER BY blockchain) AS blockchain_list
    FROM txs
    GROUP BY 1, 2
)
WHERE
    CARDINALITY(blockchain_list) > 1
GROUP BY 1, 2
)
)
WHERE pct >= 0.01