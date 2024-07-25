/*
======= Query Info =======                     
-- query_id: 3495607                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.787797                     
-- owner: hdser                     
==========================
*/

WITH
tx AS (
  SELECT
    DATE_TRUNC('hour', block_time) AS minute,
    success,
    AVG(priority_fee_per_gas) / POWER(10, 9) AS priority_fee_per_gas_gwei,
    COUNT(hash) AS cnt
  FROM gnosis.transactions
  WHERE
    block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
  GROUP BY
    1,
    2
), 
final AS (
  SELECT
    min1.minute,
    min1.priority_fee_per_gas_gwei AS included_priority_fee_per_gas_gwei,
    min2.priority_fee_per_gas_gwei AS failed_priority_fee_per_gas_gwei,
    min1.cnt AS included_cnt,
    min2.cnt AS failed_cnt
  FROM (
    SELECT
      minute,
      priority_fee_per_gas_gwei,
      cnt
    FROM tx
    WHERE
      success = TRUE
  ) AS min1
  JOIN (
    SELECT
      minute,
      priority_fee_per_gas_gwei,
      cnt
    FROM tx
    WHERE
      success = FALSE
  ) AS min2
    ON min1.minute = min2.minute
)
SELECT
  minute,
  included_priority_fee_per_gas_gwei AS "Priority Fee",
  CAST(failed_cnt AS REAL) / (
    included_cnt + failed_cnt
  ) AS "Failed Txs"
FROM final