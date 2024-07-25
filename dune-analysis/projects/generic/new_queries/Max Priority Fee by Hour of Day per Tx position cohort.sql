/*
======= Query Info =======                     
-- query_id: 3503032                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.968485                     
-- owner: hdser                     
==========================
*/

SELECT
   EXTRACT(hour FROM block_time) AS hour
  ,tx_position
  ,APPROX_PERCENTILE(CAST(max_priority_fee_per_gas AS DOUBLE),0.5)/POWER(10,9)  AS median_max_priority_fee_per_gas_gwei
  ,AVG(CAST(max_priority_fee_per_gas AS DOUBLE))/POWER(10,9)  AS mean_max_priority_fee_per_gas_gwei
FROM (
  SELECT
    block_time,
    CASE 
        WHEN "index" = 0 THEN '0' 
        WHEN "index" >= 1 AND "index" < 10  THEN '1-10' 
        WHEN "index" >= 10 AND "index" < 100  THEN '10-100' 
        ELSE '100+' 
    END AS tx_position,
    max_priority_fee_per_gas
  FROM gnosis.transactions
  WHERE
    block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
) AS subquery
GROUP BY
 1,2