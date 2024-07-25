/*
======= Query Info =======                     
-- query_id: 3729021                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.547207                     
-- owner: hdser                     
==========================
*/

WITH

dappnode_ClaimedIncentive AS (
    SELECT 
        block_time
        ,varbinary_substring(topic1,13,20) AS address
    FROM
        gnosis.logs
    WHERE
        contract_address = 0x6C68322cf55f5f025F2aebd93a28761182d077c3
        AND 
        topic0 = 0x652ee135e6d0db045dcd363ddd0952e0d36eabf828e2217d91116d6d65c6bf8c
        AND
        block_time >= DATE '2022-01-01'
),

calendar AS (
    SELECT
        day
    FROM (
        SELECT 
            CAST(MIN(block_time) AS DATE) AS block_time_min
            ,CAST(MAX(block_time) AS DATE) AS block_time_max
        FROM
            dappnode_ClaimedIncentive
        )
        ,UNNEST(SEQUENCE(block_time_min,block_time_max,INTERVAL '1' DAY)) a(day)
)

SELECT
    day
    ,SUM(wk_counts) OVER (ORDER BY day) AS wk_counts
    ,SUM(validators_counts) OVER (ORDER BY day) AS validators_counts
FROM (
    SELECT
        t3.day
        ,COALESCE(COUNT(DISTINCT t1.address),0) AS wk_counts
        ,COALESCE(SUM(t2.count),0) AS validators_counts
    FROM
        dappnode_ClaimedIncentive t1
    INNER JOIN
         dune.hdser.dataset_distinct_withdrawal_credentials_eth1_15976663 t2
        ON 
        t2.address = t1.address
    RIGHT JOIN
        calendar t3
        ON
        t3.day = DATE_TRUNC('day',t1.block_time) 
    GROUP BY 1
)