/*
======= Query Info =======                     
-- query_id: 3491754                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.705162                     
-- owner: hdser                     
==========================
*/

WITH 
block_gas AS (
  SELECT
    time AS block_time,
    "number" AS block_number,
    gas_limit,
    gas_used,
    base_fee_per_gas
  FROM gnosis.blocks
  WHERE
    time >= NOW() - interval '2' day
), 
tx_gas AS (
  SELECT
    tx.block_time,
    tx.block_number,
    tx.index,
    tx.success,
    tx.type AS tx_type,
    tx.gas_limit AS tx_gas_limit,
    tx.gas_price AS tx_gas_price,
    tx.gas_used AS tx_gas_used,
    CASE WHEN tx.type = 'Legacy' THEN tx.gas_price ELSE tx.max_fee_per_gas END AS tx_max_fee,
    CASE WHEN tx.type = 'Legacy' THEN tx.gas_price ELSE tx.max_priority_fee_per_gas END AS tx_max_priority_fee,
    CASE
      WHEN tx.type = 'Legacy'
      THEN tx.gas_price - blk.base_fee_per_gas
      ELSE tx.priority_fee_per_gas
    END AS tx_priority_fee,
    blk.base_fee_per_gas,
    blk.gas_limit,
    blk.gas_used
  FROM gnosis.transactions AS tx
  INNER JOIN block_gas AS blk
    ON blk.block_number = tx.block_number
  ORDER BY
    tx.block_number,
    tx.index
)

SELECT 
    date_trunc('minute', block_time) AS time
    ,tx_type
    ,SUM(tx_gas_used) AS tx_gas_used
    ,SUM(gas_used) AS gas_used
    ,TRY_CAST(SUM(tx_gas_used) AS DOUBLE)/TRY_CAST(SUM(gas_used) AS DOUBLE) AS weighted_cnt
FROM (
    SELECT
      block_time
      ,tx_type
      ,SUM(tx_gas_used) AS tx_gas_used
      ,ANY_VALUE(gas_used) AS gas_used
    FROM tx_gas 
    GROUP BY
      block_time
      ,tx_type

)
GROUP BY
    date_trunc('minute', block_time)
    ,tx_type
ORDER BY
  1,
  2