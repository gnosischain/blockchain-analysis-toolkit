/*
======= Query Info =======                     
-- query_id: 3713787                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.136260                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_gp_entities_balance_diff AS (
    SELECT 
        evt_block_date
        ,token_address
        ,entity_id
        ,SUM(balance_diff) AS value
    FROM query_3707217
    GROUP BY 1,2,3
)


SELECT  
    t1.evt_block_date
    ,t1.token_address
    ,(SUM(t1.value) OVER (PARTITION BY t1.token_address ORDER BY t1.evt_block_date))/POWER(10,COALESCE(t2.decimals,18)) AS value
FROM 
    gnosis_gp_entities_balance_diff t1
LEFT JOIN
    tokens.erc20 t2
    ON 
    t2.contract_address = t1.token_address