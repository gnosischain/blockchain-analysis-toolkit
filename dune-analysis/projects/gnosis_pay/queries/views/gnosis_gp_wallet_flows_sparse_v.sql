-- query_id: 3713262

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

gnosis_gp_entities_balance_diff AS (
    SELECT * FROM query_3707217
),

wallet_balance AS (
    SELECT DISTINCT
        t1.token_address
        ,t1.evt_block_date
        ,t1.entity_id
        ,'Balance' AS action
        ,SUM(t1.balance_diff) OVER (PARTITION BY t1.entity_id, t1.token_address ORDER BY t1.evt_block_date) AS value
    FROM
        gnosis_gp_entities_balance_diff t1
    INNER JOIN
        gnosis_gp_users t2
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
            gnosis_gp_entities_balance_diff t1
        INNER JOIN
            gnosis_gp_users t2
            ON
            t2.pay_wallet = t1.user
)


SELECT * FROM wallet_balance
UNION ALL 
SELECT * FROM wallet_flows