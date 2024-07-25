/*
======= Query Info =======                     
-- query_id: 3706069                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.064770                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM dune.hdser.query_3663810
  --  WHERE created_at >= DATE '2023-05-01'
),

user_transfers_inflow AS (
    SELECT
        DATE_TRUNC('day', t1.evt_block_time) AS block_day
        ,t1.contract_address AS token_address
        ,t2.safe_wallet
        ,t1."from" AS counterparty
        ,CAST(SUM(t1.value) AS DOUBLE) AS value
    FROM
        erc20_gnosis.evt_transfer t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.to
    WHERE
        t1.evt_block_time >= t2.created_at
    GROUP BY 1,2, 3,4
),

user_transfers_outflow AS (
    SELECT
        DATE_TRUNC('day', t1.evt_block_time) AS block_day
        ,t1.contract_address AS token_address
        ,t2.safe_wallet
        ,t1.to AS counterparty
        ,CAST(-SUM(t1.value) AS DOUBLE) AS value
    FROM
        erc20_gnosis.evt_transfer t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1."from"
    WHERE
        t1.evt_block_time >= t2.created_at
    GROUP BY 1,2, 3,4
),

users_transfers_netflow AS (
    SELECT
        t1.block_day
        ,t1.token_address
        ,t1.safe_wallet
        ,t1.counterparty
        ,CASE
            WHEN t2.safe_wallet IS NOT NULL THEN 1
            ELSE 0
        END AS counterparty_is_user
        ,SUM(t1.value) AS value
    FROM (
        SELECT * FROM user_transfers_inflow
        UNION ALL
        SELECT * FROM user_transfers_outflow
    ) t1
    LEFT JOIN 
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.counterparty
    GROUP BY 1,2,3,4, 5
),

circle_metadata AS (
    SELECT 
        token AS token_address
        ,CONCAT('CRC_',CAST(user AS VARCHAR)) AS symbol
        ,18 AS decimals
    FROM circles_ubi_gnosis.Hub_evt_Signup
),

tokens_metadata AS (
    SELECT 
        contract_address AS token_address
        ,symbol
        ,decimals
    FROM tokens.erc20
    WHERE  blockchain = 'gnosis' AND symbol != 'CRC'
    
    UNION ALL
    
    SELECT * FROM circle_metadata
)

SELECT 
    t1.block_day
    ,t1.token_address
    ,t1.safe_wallet
    ,t1.counterparty
    ,t1.counterparty_is_user
    ,t1.value AS value_raw
    ,t1.value/POWER(10,t2.decimals) AS value
    ,t1.value/POWER(10,t2.decimals) * t3.price AS value_USD
    ,t2.symbol
FROM users_transfers_netflow t1
LEFT JOIN
    tokens_metadata t2
    ON
    t2.token_address = t1.token_address
LEFT JOIN
    prices.usd t3
    ON t3.contract_address = t2.token_address
    AND t3.blockchain = 'gnosis'
    AND t3.minute = t1.block_day
