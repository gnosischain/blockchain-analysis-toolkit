/*
======= Query Info =======                     
-- query_id: 3493992                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.206310                     
-- owner: hdser                     
==========================
*/

SELECT
    DATE_TRUNC('day', time) AS block_day
    ,SUM(gas_limit)/POWER(10,9) AS "Gas Limit" 
    ,SUM(gas_used)/POWER(10,9) AS "Gas Used"
FROM
  gnosis.blocks
GROUP BY
    1