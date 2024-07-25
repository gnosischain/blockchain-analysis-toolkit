/*
======= Query Info =======                     
-- query_id: 3836686                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.799073                     
-- owner: hdser                     
==========================
*/

WITH

wxdai_volume AS (
    SELECT
        block_date
        ,token_address
        ,SUM(amount_raw) AS amount_raw
    FROM (
        SELECT 
            evt_block_date AS block_date
            ,contract_address AS token_address
            ,SUM(wad) AS amount_raw
        FROM 
            wxdai_gnosis.WXDAI_evt_Deposit
        WHERE
            evt_block_date >= DATE '2023-01-01'
        GROUP BY 1, 2
        
        UNION ALL
        
        SELECT 
             evt_block_date AS block_date
            ,contract_address AS token_address
            ,SUM(wad) AS amount_raw
        FROM 
            wxdai_gnosis.WXDAI_evt_Withdrawal
        WHERE
            evt_block_date >= DATE '2023-01-01'
        GROUP BY 1, 2
    )
    GROUP BY
        1, 2
),

xdai_volume AS (
    SELECT
        block_date
        ,0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee AS token_address
        ,SUM(value) AS amount_raw
    FROM gnosis.traces
    WHERE success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > UINT256 '0'
        AND
        block_date >= DATE '2023-01-01'
    GROUP BY 1
),

tokens_volume AS (
    SELECT
        evt_block_date AS block_day
        ,contract_address AS token_address
        ,SUM(value) AS amount_raw
    FROM
        erc20_gnosis.evt_transfer
    WHERE
        evt_block_date >= DATE '2023-01-01'
        AND
        value > 0
    GROUP BY 1, 2
),

volume_raw AS (
    SELECT * FROM tokens_volume
    UNION ALL
    SELECT * FROM wxdai_volume
    UNION ALL
    SELECT * FROM xdai_volume
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
    ,IF(symbol IN ('xDAI','WXDAI','sDAI','USDC','USDT','EURe','GBPe','aGnoUSDC','aGnoEURe','CRVUSD', 'aGnosDAI','BUSD','USDC.e'), 'Stablecoins', symbol) AS symbol
    --,symbol
    ,COALESCE(SUM(amount),0) AS amount
FROM volume
GROUP BY 1,2
)
)
WHERE frac >= 0.01
ORDER BY 3 DESC