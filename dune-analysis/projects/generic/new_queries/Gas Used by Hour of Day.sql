/*
======= Query Info =======                     
-- query_id: 3494006                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.480938                     
-- owner: hdser                     
==========================
*/

SELECT
    EXTRACT(HOUR FROM time) AS hour
    ,APPROX_PERCENTILE(gas_used,0.5) AS "Median" 
    ,APPROX_PERCENTILE(gas_used,0.1) AS "10th Percentile" 
    ,APPROX_PERCENTILE(gas_used,0.9) AS "90th Percentile" 
    ,AVG(gas_used) AS "Mean" 
FROM
  gnosis.blocks
WHERE
  date > NOW() - INTERVAL '1' MONTH
GROUP BY
    1