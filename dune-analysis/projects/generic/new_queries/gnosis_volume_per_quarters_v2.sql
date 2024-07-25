/*
======= Query Info =======                     
-- query_id: 3926001                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:57.411971                     
-- owner: hdser                     
==========================
*/

WITH

volume_raw AS (
    SELECT
        block_date AS block_day
        ,CASE 
            WHEN token_standard = 'native' THEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
            ELSE contract_address
        END AS token_address
        ,SUM(amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_c67e8dd_tokens_gnosis_transfers
        --tokens_gnosis.transfers
    WHERE
        block_date >= DATE '2023-01-01'
        AND
        amount_raw > 0
    GROUP BY 1,2
),

volume AS (
        SELECT 
            t1.block_day
            ,CASE
                WHEN t1.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'xDAI'
                WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 THEN 'GBPe'
                WHEN t1.token_address = 0xc6B7AcA6DE8a6044E0e32d0c841a89244A10D284 THEN 'aGnoUSDC'
                WHEN t1.token_address = 0xEdBC7449a9b594CA4E053D9737EC5Dc4CbCcBfb2 THEN 'aGnoEURe'
                WHEN t1.token_address = 0x7a5c3860a77a8DC1b225BD46d0fb2ac1C6D191BC THEN 'aGnosDAI'
                ELSE t2.symbol
            END AS symbol
            ,SUM(
                CASE
                    WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 
                        THEN t1.amount_raw/POWER(10,18)*1.25
                    WHEN t1.token_address = 0xc6B7AcA6DE8a6044E0e32d0c841a89244A10D284
                        THEN t1.amount_raw/POWER(10,6)*1.0
                    WHEN t1.token_address = 0x7a5c3860a77a8DC1b225BD46d0fb2ac1C6D191BC
                        THEN t1.amount_raw/POWER(10,18)*1.0
                    WHEN t1.token_address = 0xEdBC7449a9b594CA4E053D9737EC5Dc4CbCcBfb2
                        THEN t1.amount_raw/POWER(10,18)*t2.price
                    ELSE  t1.amount_raw/POWER(10,t2.decimals)*t2.price
                END
            ) AS amount
        FROM volume_raw t1
        LEFT JOIN
            prices.usd_daily t2
            ON t2.blockchain = 'gnosis'
            AND t2.day = t1.block_day
            AND
            (
            t2.contract_address = t1.token_address
            OR 
            (t2.contract_address = 0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d 
                AND 
            t1.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)
            OR 
            (t2.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E 
                AND 
            t1.token_address = 0xEdBC7449a9b594CA4E053D9737EC5Dc4CbCcBfb2)
            )
        GROUP BY 1,2
)

SELECT
    block_date
    ,symbol
    ,amount
FROM (
SELECT
    block_date
    ,symbol
    ,amount
    ,amount/(SUM(amount) OVER (PARTITION BY block_date) ) AS frac
FROM (
SELECT 
    CONCAT(CAST(YEAR(block_day) AS VARCHAR),' - Q', CAST(QUARTER(block_day) AS VARCHAR)) AS block_date
    ,symbol
    ,COALESCE(SUM(amount),0) AS amount
FROM volume
GROUP BY 1,2
)
)
WHERE frac >= 0.01
ORDER BY 3 DESC
