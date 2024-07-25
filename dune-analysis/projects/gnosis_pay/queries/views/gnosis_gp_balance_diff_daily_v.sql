/*
======= Query Info =======                     
-- query_id: 3814057                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.526117                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

tokens_balances_diff AS (
    SELECT
        t1.block_day
        ,t1.token_address
        ,t2.entity_id
        ,'Wallet' AS label
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_3334f50_balances_diff_gnosis_erc20_sparse_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.pay_wallet = t1.wallet_address
    GROUP BY 1,2,3,4
    
    UNION ALL
    
    SELECT
        t1.block_day
        ,t1.token_address
        ,t2.entity_id
        ,'Owner' AS label
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_3334f50_balances_diff_gnosis_erc20_sparse_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.owner = t1.wallet_address
    GROUP BY 1,2,3,4
),

xdai_balances_diff AS (
    SELECT
        block_day
        ,t1.token_address
        ,t2.entity_id
        ,'Wallet' AS label
        ,SUM(t1.amount_raw_diff) AS amount_raw
    FROM
        test_schema.git_dunesql_3334f50_balances_diff_gnosis_xdai_sparse_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.pay_wallet = t1.wallet_address
    GROUP BY 1,2,3,4
        
    UNION ALL
    
    SELECT
        block_day
        ,t1.token_address
        ,t2.entity_id
        ,'Owner' AS label
        ,SUM(t1.amount_raw_diff) AS amount_raw
    FROM
        test_schema.git_dunesql_3334f50_balances_diff_gnosis_xdai_sparse_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.owner = t1.wallet_address
    GROUP BY 1,2,3,4
),

balances_diff AS (
    SELECT * FROM tokens_balances_diff
    UNION ALL
    SELECT * FROM xdai_balances_diff
)

SELECT * FROM balances_diff