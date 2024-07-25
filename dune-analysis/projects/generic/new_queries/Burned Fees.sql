/*
======= Query Info =======                     
-- query_id: 3495798                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.389044                     
-- owner: hdser                     
==========================
*/

SELECT
    SUM(base_fee_per_gas * gas_used) /POWER(10,18) AS burned_fee
FROM
  gnosis.blocks
WHERE
  time >= NOW() - INTERVAL '24' HOUR