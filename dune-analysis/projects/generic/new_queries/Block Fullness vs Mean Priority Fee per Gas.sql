/*
======= Query Info =======                     
-- query_id: 3495331                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.571882                     
-- owner: hdser                     
==========================
*/

WITH

block_gas AS (
    SELECT
        date_trunc('day', time) AS block_day
        ,AVG(CAST(gas_used AS REAL)/gas_limit) AS avg_fullness
    FROM
        gnosis.blocks
    GROUP BY
        1
),

tx_gas AS (
    SELECT
     date_trunc('day', block_time) AS block_day
    ,AVG(priority_fee_per_gas)/POWER(10,9) AS avg_priority_fee_per_gas_gwei
    FROM 
        gnosis.transactions
    GROUP BY
        1
)

SELECT
    tx.block_day
    ,blk.avg_fullness AS "Block Fullness"
    ,tx.avg_priority_fee_per_gas_gwei AS "Priority Fee"
FROM
    tx_gas tx
INNER JOIN
    block_gas blk
    ON
    blk.block_day = tx.block_day
