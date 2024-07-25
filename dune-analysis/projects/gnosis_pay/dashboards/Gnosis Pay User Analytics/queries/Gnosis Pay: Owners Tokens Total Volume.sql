/*
======= Query Info =======                 
-- query_id: 3741698                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:45.955927                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

tokens_volume_raw AS (
    SELECT
        t1.block_hour
        ,t1.token_address
        ,SUM(
            IF(t3.pay_wallet IS NULL, ABS(t1.amount_raw), ABS(t1.amount_raw)/2) 
        ) AS amount_raw
    FROM
        test_schema.git_dunesql_075f38f_transfers_gnosis_erc20_agg_hour t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.owner = t1.wallet_address
    LEFT JOIN
        gnosis_gp_users t3
        ON 
        t2.owner = t1.counterparty
    WHERE
        DATE_TRUNC('day',t1.block_hour) >= CURRENT_DATE - INTERVAL '14' DAY
    GROUP BY 1,2
),

xdai_volume_raw AS (
    SELECT
        t1.block_hour
        ,t1.token_address
        ,SUM(
            IF(t3.pay_wallet IS NULL, ABS(t1.amount_raw), ABS(t1.amount_raw)/2) 
        ) AS amount_raw
    FROM
        test_schema.git_dunesql_075f38f_transfers_gnosis_xdai_agg_hour t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.owner = t1.wallet_address
    LEFT JOIN
        gnosis_gp_users t3
        ON 
        t2.owner = t1.counterparty
    WHERE
        DATE_TRUNC('day',t1.block_hour) >= CURRENT_DATE - INTERVAL '14' DAY
    GROUP BY 1,2
),

volume_raw AS (
    SELECT * FROM tokens_volume_raw
    UNION ALL
    SELECT * FROM xdai_volume_raw
),

calendar AS (
    SELECT 
        token_address
        ,block_hour
    FROM (
        SELECT 
            t1.token_address
            ,MIN(t1.block_hour) AS block_hour_min
        FROM
            volume_raw t1
        GROUP BY 1
    )
    ,UNNEST(SEQUENCE(CAST(block_hour_min AS TIMESTAMP),CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL '1' HOUR)) s(block_hour)
),

volume_raw_dense AS (
    SELECT 
        t2.block_hour
        ,t2.token_address
        ,COALESCE(t1.amount_raw,0) AS amount_raw
    FROM
        volume_raw t1
    RIGHT JOIN
        calendar t2
        ON
        t2.block_hour = t1.block_hour
        AND 
        t2.token_address = t1.token_address
),

volume AS (
        SELECT 
            t1.block_hour
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
        FROM volume_raw_dense t1
        LEFT JOIN
            prices.usd t2
            ON t2.blockchain = 'gnosis'
            AND t2.minute = t1.block_hour
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

SELECT * FROM volume
WHERE 
    amount!=0
    AND
    block_hour < CURRENT_DATE
ORDER BY 
    amount DESC
