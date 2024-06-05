--query_id: 3731893

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

gnosis_gp_entities_balance_diff AS (
    SELECT * FROM query_3707217
),

wallet_owner_flows AS (
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
            AND
            t2.owner = t1.counterparty
            
)


SELECT * FROM wallet_owner_flows