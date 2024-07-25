/*
======= Query Info =======                     
-- query_id: 3491966                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.674621                     
-- owner: hdser                     
==========================
*/

WITH

block_gas AS (
    SELECT
        time AS block_time
        ,"number" AS block_number
        ,gas_limit
        ,gas_used
        ,base_fee_per_gas
    FROM
        gnosis.blocks
),

tx_gas AS (
SELECT
     tx.block_time
    ,tx.block_number
    ,tx.index
    ,tx.success
    ,tx.type AS tx_type
    ,tx.gas_limit AS tx_gas_limit
    ,tx.gas_price AS tx_gas_price
    ,tx.gas_used AS tx_gas_used
    ,CASE
        WHEN tx.type = 'DynamicFee' THEN tx.max_fee_per_gas 
        ELSE tx.gas_price
    END AS tx_max_fee
    ,CASE
        WHEN tx.type = 'DynamicFee' THEN tx.max_priority_fee_per_gas
        ELSE tx.gas_price 
    END AS tx_max_priority_fee
    ,CASE
        WHEN type = 'DynamicFee' THEN tx.priority_fee_per_gas 
        ELSE tx.gas_price - blk.base_fee_per_gas
    END AS  tx_priority_fee
FROM 
    gnosis.transactions tx
INNER JOIN 
    block_gas blk
    ON 
    blk.block_number = tx.block_number
ORDER BY  
    tx.block_number
    ,tx.index
),

tx_gas_res AS (
    SELECT
      date_trunc('day', block_time) AS block_day
      ,tx_type
      ,COUNT(1) AS cnt
      ,SUM(TRY_CAST(tx_gas_used AS DOUBLE))/POWER(10,9) AS total_tx_gas_used_gwei
      ,AVG(TRY_CAST(tx_gas_price AS DOUBLE))/POWER(10,9) AS avg_tx_gas_price_gwei
      ,MAX(TRY_CAST(tx_gas_price AS DOUBLE))/POWER(10,9) AS max_tx_gas_price_gwei
      ,MIN(TRY_CAST(tx_gas_price AS DOUBLE))/POWER(10,9) AS min_tx_gas_price_gwei
      ,APPROX_PERCENTILE(TRY_CAST(tx_gas_price AS DOUBLE),0.5)/POWER(10,9) AS median_tx_gas_price_gwei
      
      ,AVG(TRY_CAST(tx_max_fee AS DOUBLE))/POWER(10,9) AS avg_tx_max_fee_gwei
      ,MAX(TRY_CAST(tx_max_fee AS DOUBLE))/POWER(10,9) AS max_tx_max_fee_gwei
      ,MIN(TRY_CAST(tx_max_fee AS DOUBLE))/POWER(10,9) AS min_tx_max_fee_gwei
      ,APPROX_PERCENTILE(TRY_CAST(tx_max_fee AS DOUBLE),0.5)/POWER(10,9) AS median_tx_max_fee_gwei
    FROM 
        tx_gas 
    GROUP BY
        date_trunc('day', block_time)
        ,tx_type
),

block_gas_res AS (
    SELECT
        date_trunc('day', block_time) AS block_day
        ,APPROX_PERCENTILE(TRY_CAST(gas_limit AS DOUBLE),0.5)/POWER(10,9) AS median_gas_limit_gwei
        ,AVG(TRY_CAST(gas_limit AS DOUBLE))/POWER(10,9) AS avg_gas_limit_gwei
        ,SUM(TRY_CAST(gas_used AS DOUBLE))/POWER(10,9) AS total_gas_used_gwei
        ,APPROX_PERCENTILE(TRY_CAST(base_fee_per_gas AS DOUBLE),0.5)/POWER(10,9) AS median_base_fee_gwei
        ,AVG(TRY_CAST(base_fee_per_gas AS DOUBLE))/POWER(10,9) AS avg_base_fee_gwei
    FROM
        block_gas
    GROUP BY
        date_trunc('day', block_time)
)

SELECT 
    tx.block_day
    ,tx_type
    ,cnt
    ,total_tx_gas_used_gwei/total_gas_used_gwei AS weighted_cnt
    ,total_tx_gas_used_gwei
    ,total_gas_used_gwei
    ,avg_tx_gas_price_gwei
    ,max_tx_gas_price_gwei
    ,min_tx_gas_price_gwei
    ,median_tx_gas_price_gwei
    
    ,avg_tx_max_fee_gwei
    ,max_tx_max_fee_gwei
    ,min_tx_max_fee_gwei
    ,median_tx_max_fee_gwei
FROM
    tx_gas_res tx
INNER JOIN
    block_gas_res blk
    ON
    blk.block_day = tx.block_day
ORDER BY 1,2