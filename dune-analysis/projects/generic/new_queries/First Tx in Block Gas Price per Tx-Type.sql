/*
======= Query Info =======                     
-- query_id: 3493239                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.456798                     
-- owner: hdser                     
==========================
*/

SELECT
     date_trunc('hour', block_time) AS hour
    ,type AS tx_type
    ,APPROX_PERCENTILE(gas_price,0.5)/POWER(10,9) AS median_tx_gas_price_gwei
FROM 
    gnosis.transactions
WHERE
    block_time >= NOW() - INTERVAL '24' HOUR
    AND
    index = 0
GROUP BY
    date_trunc('hour', block_time)
    ,type
