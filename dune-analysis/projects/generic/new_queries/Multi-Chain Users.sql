/*
======= Query Info =======                     
-- query_id: 3904848                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=from_date, value=2021-07-01 00:00:00, type=datetime), Parameter(name=to_date, value=2024-07-08 00:00:00, type=datetime)]                     
-- last update: 2024-07-25 17:22:53.792157                     
-- owner: hdser                     
==========================
*/

WITH

txs AS (
SELECT DISTINCT
    t1.blockchain 
    ,t1."from" AS tx_from
FROM evms.transactions t1
WHERE
    t1.block_time >= TIMESTAMP '{{from_date}}'
    AND
    t1.block_time <= TIMESTAMP '{{to_date}}'
)


SELECT
    CARDINALITY(blockchain_list) AS nchains
    ,COUNT(*) AS cnt
FROM (
    SELECT
        tx_from
        ,ARRAY_AGG(blockchain ORDER BY blockchain) AS blockchain_list
    FROM txs
    GROUP BY 1
)
WHERE
    CARDINALITY(blockchain_list) > 2
GROUP BY 1

