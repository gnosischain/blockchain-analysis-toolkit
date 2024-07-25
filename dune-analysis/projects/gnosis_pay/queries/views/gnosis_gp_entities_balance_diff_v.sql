/*
======= Query Info =======                 
-- query_id: 3707217                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:45.418197                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804 -- dune.hdser.result_gnosis_gp_users_mv --
),

wallet_tokens_outflow AS (
    SELECT 
        t1.contract_address AS token_address
        ,t1.evt_block_date
        ,t2.entity_id
        ,t1."from" AS user
        ,t1.to AS counterparty
        ,SUM(-TRY_CAST(value AS INT256)) AS balance_diff
    FROM
        erc20_gnosis.evt_transfer t1
    INNER JOIN
        gnosis_gp_users t2
        ON
        t2.pay_wallet = t1."from"
    WHERE
        t2.creation_time <= t1.evt_block_time
    GROUP BY 1,2,3,4,5
),

wallet_tokens_inflow AS (
    SELECT 
        t1.contract_address AS token_address
        ,t1.evt_block_date
        ,t2.entity_id
        ,t1.to AS user
        ,t1."from" AS counterparty
        ,SUM(TRY_CAST(value AS INT256)) AS balance_diff
    FROM
        erc20_gnosis.evt_transfer t1
    INNER JOIN
        gnosis_gp_users t2
        ON
        t2.pay_wallet = t1.to
        --CONTAINS(t2.wallet_entity, t1.to)
    WHERE
        t2.creation_time <= t1.evt_block_time
    GROUP BY 1,2,3,4,5
),

owner_tokens_outflow AS (
    SELECT 
        t1.contract_address AS token_address
        ,t1.evt_block_date
        ,t2.entity_id
        ,t1."from" AS user
        ,t1.to AS counterparty
        ,SUM(-TRY_CAST(value AS INT256)) AS balance_diff
    FROM
        erc20_gnosis.evt_transfer t1
    INNER JOIN
        gnosis_gp_users t2
        ON
        t2.owner = t1."from"
    WHERE
        t2.creation_time <= t1.evt_block_time
    GROUP BY 1,2,3,4,5
),

owner_tokens_inflow AS (
    SELECT 
        t1.contract_address AS token_address
        ,t1.evt_block_date
        ,t2.entity_id
        ,t1.to AS user
        ,t1."from" AS counterparty
        ,SUM(TRY_CAST(value AS INT256)) AS balance_diff
    FROM
        erc20_gnosis.evt_transfer t1
    INNER JOIN
        gnosis_gp_users t2
        ON
        t2.owner = t1.to
    WHERE
        t2.creation_time <= t1.evt_block_time
    GROUP BY 1,2,3,4,5
),

tokens_netflow AS (
    SELECT 
    *
    ,CASE
        WHEN counterparty = 0x0000000000000000000000000000000000000000 
            THEN IF(balance_diff < 0, 'Burn', 'Mint') 
        ELSE IF(balance_diff < 0, 'Outflow', 'Inflow')
    END AS action
    FROM (
        SELECT * FROM wallet_tokens_outflow
        UNION ALL
        SELECT * FROM wallet_tokens_inflow
        UNION ALL
        SELECT * FROM owner_tokens_outflow
        UNION ALL
        SELECT * FROM owner_tokens_inflow
    )
    
)

SELECT * FROM tokens_netflow
