/*
======= Query Info =======                 
-- query_id: 3735920                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.772643                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM dune.hdser.query_3663810
    WHERE created_at >= DATE '2024-05-01'
),

balances_diff AS (
    SELECT
        block_day
        ,token_address
        ,SUM(amount_raw) AS amount_raw
    FROM (
    SELECT
        t1.block_day
        ,t1.token_address
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_075f38f_transfers_gnosis_erc20_agg_day t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.block_day >= DATE_TRUNC('day',t2.created_at)
    GROUP BY 1,2
        
    UNION ALL
        
    SELECT
        t1.block_day
        ,t1.token_address
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_075f38f_transfers_gnosis_xdai_agg_day t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.block_day >= DATE_TRUNC('day',t2.created_at)
    GROUP BY 1,2
    )
    GROUP BY 1,2
),

circle_metadata AS (
    SELECT 
        token AS token_address
        ,CONCAT('CRC_',CAST(user AS VARCHAR)) AS symbol
        ,18 AS decimals
    FROM circles_ubi_gnosis.Hub_evt_Signup
),

calendar AS (
    SELECT 
        token_address
        ,block_day
    FROM (
        SELECT 
            t1.token_address
            ,MIN(t1.block_day) AS block_day_min
        FROM
            balances_diff t1
        WHERE
            t1.token_address NOT IN (SELECT token_address FROM circle_metadata)
        GROUP BY 1
    )
    ,UNNEST(SEQUENCE(CAST(block_day_min AS DATE),CURRENT_DATE, INTERVAL '1' DAY)) s(block_day)
),

balances_diff_dense AS (
    SELECT 
        t2.block_day
        ,t2.token_address
        ,COALESCE(t1.amount_raw,0) AS amount_raw
    FROM
        balances_diff t1
    RIGHT JOIN
        calendar t2
        ON
        t2.block_day = t1.block_day
        AND 
        t2.token_address = t1.token_address
),

balances AS (
    SELECT
        block_day
        ,symbol
        ,SUM(amount) OVER (PARTITION BY symbol ORDER BY block_day) AS amount
    FROM (
        SELECT 
            t1.block_day
            ,CASE
                WHEN t1.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'xDAI'
                WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 THEN 'GBPe'
                ELSE t2.symbol
            END AS symbol
            ,SUM(
                CASE
                    WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 
                        THEN t1.amount_raw/POWER(10,18)*1.25
                    ELSE  t1.amount_raw/POWER(10,t2.decimals)*t2.price
                END
            ) AS amount
        FROM balances_diff_dense t1
        LEFT JOIN
            prices.usd t2
            ON t2.blockchain = 'gnosis'
            AND t2.minute = t1.block_day
            AND
            (
            t2.contract_address = t1.token_address
            OR 
            (t2.contract_address = 0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d 
                AND 
            t1.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)
            )
        GROUP BY 1,2
    )
)

SELECT * FROM balances
WHERE
    block_day >= CURRENT_DATE - INTERVAL '1' YEAR
    AND
    block_day < CURRENT_DATE
