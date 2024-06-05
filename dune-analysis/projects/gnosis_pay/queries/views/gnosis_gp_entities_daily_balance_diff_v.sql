-- query_id: 3796435

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804 -- dune.hdser.result_gnosis_gp_users_mv --
),

wallet_tokens_flow AS (
    SELECT 
         t1.token_address
        ,t1.block_day AS block_date
        ,t2.entity_id
        ,t1.wallet_address AS user
        ,t1.counterparty
        ,t1.amount_raw AS balance_diff
    FROM
        test_schema.git_dunesql_11470a0_transfers_gnosis_erc20_agg_day  t1
    INNER JOIN
        gnosis_gp_users t2
        ON
        t2.pay_wallet = t1.wallet_address
        OR
        t2.owner = t1.wallet_address
)

SELECT 
    token_address
    ,block_date
    ,entity_id
    ,user
    ,counterparty
    ,balance_diff
    ,CASE
        WHEN counterparty = 0x0000000000000000000000000000000000000000 
            THEN IF(balance_diff < 0, 'Burn', 'Mint') 
        ELSE IF(balance_diff < 0, 'Outflow', 'Inflow')
    END AS action
FROM wallet_tokens_flow