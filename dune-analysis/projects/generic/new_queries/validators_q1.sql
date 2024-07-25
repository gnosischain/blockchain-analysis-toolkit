/*
======= Query Info =======                     
-- query_id: 3729247                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.063205                     
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
),

size_cohorts AS (
    SELECT
        lower
        ,upper
        ,IF(upper=10000000,
            concat(CAST(lower AS VARCHAR),'>='),
            concat(CAST(lower AS VARCHAR),'-', CAST(upper AS VARCHAR))) AS cohort
    FROM
        UNNEST(ARRAY[1,10,50,100,200,300,400,500,600,700,800,900,1000]) WITH ORDINALITY a(lower,idx)
        ,UNNEST(ARRAY[10,50,100,200,300,400,500,600,700,800,900,1000,10000000]) WITH ORDINALITY s(upper,idx)
    WHERE
        a.idx = s.idx
)


    SELECT
       cohort
       ,COUNT(DISTINCT address) AS cnt
    FROM (
        SELECT
            t3.cohort
            ,t1.address 
            ,t2.count
        FROM
            dappnode_ClaimedIncentive t1
        INNER JOIN
             dune.hdser.dataset_eth1_withdrawal_credentials_gnosis_slot_15338875 t2
            ON 
            t2.address = t1.address
        CROSS JOIN
            size_cohorts t3
        WHERE
            t2.count >= t3.lower AND  t2.count < t3.upper 
    ) 
    GROUP BY 1
    