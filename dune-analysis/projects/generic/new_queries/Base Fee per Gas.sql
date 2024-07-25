/*
======= Query Info =======                     
-- query_id: 3494025                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.807448                     
-- owner: hdser                     
==========================
*/

SELECT
    AVG(base_fee_per_gas) AS avg_base_fee_per_gas
    ,APPROX_PERCENTILE(base_fee_per_gas,0.5) AS median_base_fee_per_gas
FROM
  gnosis.blocks
WHERE
  date > NOW() - INTERVAL '24' HOUR