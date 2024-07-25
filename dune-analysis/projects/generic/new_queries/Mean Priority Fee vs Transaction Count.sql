/*
======= Query Info =======                     
-- query_id: 3495446                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.243926                     
-- owner: hdser                     
==========================
*/


SELECT
     date_trunc('minute', block_time) AS minute
    ,AVG(priority_fee_per_gas)/POWER(10,9) AS "Priority Fee"
    ,COUNT(hash) AS "Txs"
FROM 
    gnosis.transactions
WHERE
    block_time >= NOW() - INTERVAL '7' DAY
GROUP BY
       1
