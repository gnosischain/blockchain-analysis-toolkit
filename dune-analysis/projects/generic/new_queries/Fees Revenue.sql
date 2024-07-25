/*
======= Query Info =======                     
-- query_id: 3495900                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.834707                     
-- owner: hdser                     
==========================
*/

WITH
blocks AS (
    SELECT
        time as block_time
        ,base_fee_per_gas
    FROM
      gnosis.blocks
    WHERE
      time >= NOW() - INTERVAL '24' HOUR
)

SELECT
    SUM(CAST((tx.gas_price - blk.base_fee_per_gas) AS DOUBLE) * tx.gas_used) /POWER(10,18) As revenue_value
FROM
  gnosis.transactions tx
INNER JOIN
    blocks blk
    ON
    blk.block_time = tx.block_time

