/*
======= Query Info =======                     
-- query_id: 3551980                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.068343                     
-- owner: hdser                     
==========================
*/

WITH 
    final AS (
SELECT 
        blockchain
        ,COUNT(week) OVER (PARTITION BY blockchain ORDER BY week) AS week_age
        ,SUM(cnt) OVER (PARTITION BY blockchain ORDER BY week) AS txs
FROM (
        SELECT 
            blockchain
            ,DATE_TRUNC('month',block_time) AS week  
            ,COUNT(*) AS cnt
        FROM evms.transactions
        WHERE 
             blockchain != 'goerli'
        --    success = TRUE
        GROUP BY 
            1, 2
    )
    )
    
SELECT
blockchain 
,MIN(week_age) AS age
FROM final
WHERE
txs >= 200e6
GROUP BY 1