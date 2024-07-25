/*
======= Query Info =======                     
-- query_id: 3793135                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.000269                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_metri_monerium_transfers AS (
    SELECT
        *
    FROM
        query_3792681
),

eure_transfers AS (
    SELECT
        CAST(evt_block_time AS DATE) AS block_day
        ,SUM(amount_raw/POWER(10,18)) AS value_eure
    FROM
        gnosis_metri_monerium_transfers
    WHERE
        token_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E
    GROUP BY 
        1
),

gbpe_transfers AS (
    SELECT
        CAST(evt_block_time AS DATE) AS block_day
        ,SUM(amount_raw/POWER(10,18)) AS value_gbpe
    FROM
        gnosis_metri_monerium_transfers
    WHERE
        token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108
    GROUP BY 
        1
),

calendar AS (
    SELECT
        block_day
    FROM (
        SELECT
            MIN(CAST(evt_block_time AS DATE)) AS min_block_day
        FROM
            gnosis_metri_monerium_transfers
    )
    ,UNNEST(SEQUENCE(min_block_day,CURRENT_DATE, INTERVAL '1' DAY)) s(block_day)
)

SELECT 
    t1.block_day
    ,COALESCE(t2.value_eure,0) AS "EURe"
    ,COALESCE(t3.value_gbpe,0) AS "GBPe"
    ,SUM(COALESCE(t2.value_eure,0)) OVER (ORDER BY t1.block_day ) AS cumsum_eure
    ,SUM(COALESCE(t3.value_gbpe,0)) OVER (ORDER BY t1.block_day ) AS cumsum_gbpe
FROM 
    calendar t1
LEFT JOIN
    eure_transfers t2
    ON 
    t2.block_day = t1.block_day
LEFT JOIN
    gbpe_transfers t3
    ON 
    t3.block_day = t1.block_day
ORDER BY 
    t1.block_day DESC