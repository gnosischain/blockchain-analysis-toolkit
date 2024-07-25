/*
======= Query Info =======                     
-- query_id: 3493877                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:57.591561                     
-- owner: hdser                     
==========================
*/

SELECT
    DATE_TRUNC('day', time) AS block_day
    ,APPROX_PERCENTILE(gas_used, 0.5) AS "Gas Used" 
    ,APPROX_PERCENTILE(gas_limit, 0.5) AS "Gas Limit"
FROM
  gnosis.blocks
GROUP BY
    DATE_TRUNC('day', time)