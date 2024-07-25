/*
======= Query Info =======                     
-- query_id: 3493298                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.838857                     
-- owner: hdser                     
==========================
*/


SELECT
  DATE_TRUNC('hour', block_time) AS hour
  ,tx_position
  ,APPROX_PERCENTILE(gas_price, 0.5) / POWER(10, 9) AS median_tx_gas_price_gwei
  ,AVG(gas_price) / POWER(10, 9) AS avg_tx_gas_price_gwei
  ,MAX(gas_price) / POWER(10, 9) AS max_tx_gas_price_gwei
FROM (
  SELECT
    block_time,
    CASE 
        WHEN "index" = 0 THEN '0' 
        WHEN "index" >= 1 AND "index" < 10  THEN '1-10' 
        WHEN "index" >= 10 AND "index" < 100  THEN '10-100' 
        ELSE '100+' 
    END AS tx_position,
    gas_price
  FROM gnosis.transactions
  WHERE
    block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
) AS subquery
GROUP BY
  DATE_TRUNC('hour', block_time),
  tx_position
  

  