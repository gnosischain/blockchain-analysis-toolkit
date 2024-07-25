/*
======= Query Info =======                     
-- query_id: 3817332                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.325467                     
-- owner: hdser                     
==========================
*/

SELECT 
     DATE_TRUNC('hour', block_time) AS block_hour
    ,address
    ,label
    ,token_standard
    ,token_address
    ,symbol
    ,SUM(amount_raw) AS amount_raw
    ,SUM(amount) AS amount
    ,SUM(amount_usd) AS amount_usd
FROM 
    query_3870527 --gnosis_ai_agents_balance_diff_v
GROUP BY
    1, 2, 3, 4, 5, 6