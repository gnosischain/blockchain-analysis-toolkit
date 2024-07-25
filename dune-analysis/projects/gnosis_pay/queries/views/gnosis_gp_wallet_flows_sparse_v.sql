/*
======= Query Info =======                 
-- query_id: 3713262                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:45.482714                 
-- owner: hdser                 
==========================
*/

WITH

wallet_balance AS (
    SELECT DISTINCT
        t1.token_address
        ,t1.evt_block_date
        ,t1.entity_id
        ,'Balance' AS action
        ,SUM(t1.balance_diff) OVER (PARTITION BY t1.entity_id, t1.token_address ORDER BY t1.evt_block_date) AS value
    FROM
        query_3707217 t1 --gnosis_gp_entities_balance_diff
    INNER JOIN
        query_3707804 t2 --gnosis_gp_users
        ON
        t2.pay_wallet = t1.user
),

wallet_flows AS (
        SELECT
            t1.token_address
            ,t1.evt_block_date
            ,t1.entity_id
            ,t1.action
            ,t1.balance_diff AS value
        FROM
            query_3707217 t1 --gnosis_gp_entities_balance_diff
        INNER JOIN
            query_3707804 t2 --gnosis_gp_users
            ON
            t2.pay_wallet = t1.user
)


SELECT * FROM wallet_balance
UNION ALL 
SELECT * FROM wallet_flows


