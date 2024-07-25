/*
======= Query Info =======                     
-- query_id: 3503011                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.638131                     
-- owner: hdser                     
==========================
*/

WITH
blocks AS (
    SELECT
        time as block_time
        ,COALESCE(base_fee_per_gas,0) AS base_fee_per_gas
    FROM
      gnosis.blocks
  
),

daily_rev AS (
SELECT 
    DATE_TRUNC('week', block_time) AS block_day
    ,SUM(revenue_value) AS "Revenue"
    ,SUM(burned_value) AS "Burned"
FROM (
    SELECT
        tx.block_time
        ,CAST((tx.gas_price - blk.base_fee_per_gas) AS DOUBLE) * tx.gas_used /POWER(10,18) As revenue_value
        ,CAST(blk.base_fee_per_gas AS DOUBLE) * tx.gas_used /POWER(10,18) As burned_value
    FROM
      gnosis.transactions tx
    INNER JOIN
        blocks blk
        ON
        blk.block_time = tx.block_time
)
GROUP BY 
    1
)

SELECT
     block_day
    ,"Revenue"
    ,"Burned"
    ,SUM("Revenue") OVER w AS "Cumulative Revenue"
    ,SUM("Burned") OVER w AS "Cumulative Burned"
FROM
    daily_rev
WINDOW w AS (ORDER BY block_day)