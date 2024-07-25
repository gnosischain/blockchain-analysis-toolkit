/*
======= Query Info =======                 
-- query_id: 3702361                 
-- description: ""                 
-- tags: []                 
-- parameters: [Parameter(name=fixedproductmarketmaker, value=0x3d2ab113682e5b66b94b1c3043f5885471e72036, type=enum)]                 
-- last update: 2024-07-25 17:22:44.130801                 
-- owner: hdser                 
==========================
*/

WITH

omen_gnosis_trades AS (
    SELECT * FROM omen_gnosis.trades
     WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

omen_gnosis_liquidity AS (
    SELECT * FROM omen_gnosis.liquidity
     WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

omen_gnosis_markets AS (
    SELECT * FROM query_3668567
    WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

trades_counts AS (
    SELECT 
        COALESCE(COUNT(DISTINCT address),0) AS cnt_traders
        ,COALESCE(COUNT(CASE WHEN action = 'Buy' THEN 1 END),0) AS cnt_buys
        ,COALESCE(COUNT(CASE WHEN action = 'Sell' THEN 1 END),0) AS cnt_sells
        ,COALESCE(SUM(CASE WHEN action = 'Sell' THEN amount/POWER(10,t3.decimals) * t3.price  END),0) As amount_sell
        ,COALESCE(SUM(CASE WHEN action = 'Buy' THEN (amount - feeAmount)/POWER(10,t3.decimals) * t3.price END),0) As amount_buy
        ,COALESCE(SUM(feeAmount/POWER(10,t3.decimals) * t3.price),0) AS fees
    FROM omen_gnosis_trades t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        prices.usd t3
        ON t3.contract_address = t2.collateraltoken
        AND t3.blockchain = 'gnosis'
        AND t3.minute = DATE_TRUNC('minute',t1.block_time)
),

liquidity_counts AS (
    SELECT
        t2.question
        ,COALESCE(COUNT(DISTINCT CASE WHEN action = 'Add' THEN tx_to END),0) AS cnt_makers
        ,COALESCE(COUNT(CASE WHEN action = 'Add' THEN 1 END),0) AS cnt_add
        ,COALESCE(COUNT(CASE WHEN action = 'Remove' THEN 1 END),0) AS cnt_remove
        ,COALESCE(SUM(CASE WHEN action = 'Add' THEN shares/POWER(10,t3.decimals)  END),0) As shares_minted
        ,COALESCE(SUM(CASE WHEN action = 'Remove' THEN shares/POWER(10,t3.decimals) END),0) As shares_burned
    FROM omen_gnosis_liquidity t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        prices.usd t3
        ON t3.contract_address = t2.collateraltoken
        AND t3.blockchain = 'gnosis'
        AND t3.minute = DATE_TRUNC('minute',t1.block_time)
    GROUP BY 1
)

SELECT t1.*, t2.* FROM trades_counts t1
RIGHT JOIN liquidity_counts t2
ON 1=1