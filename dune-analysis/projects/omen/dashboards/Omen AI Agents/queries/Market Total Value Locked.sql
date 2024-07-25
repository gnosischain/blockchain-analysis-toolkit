/*
======= Query Info =======                 
-- query_id: 3702249                 
-- description: ""                 
-- tags: []                 
-- parameters: [Parameter(name=fixedproductmarketmaker, value=0x3d2ab113682e5b66b94b1c3043f5885471e72036, type=enum)]                 
-- last update: 2024-07-25 17:22:43.508609                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_omen_markets_tvl AS (
    SELECT
        block_time
        ,evt_index
        ,tvl
        ,tvl_usd
    FROM query_3668377
    WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

resolution AS (
    SELECT
        IF(DATE_DIFF('hour',start_time,CURRENT_TIMESTAMP)>=10000, 'day', 'hour') AS step
    FROM (
        SELECT 
            MIN(block_time) AS start_time 
        FROM 
           gnosis_omen_markets_tvl
    )
),

omen_gnosis_markets AS (
    SELECT * FROM query_3668567
    WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

omen_gnosis_markets_status AS (
    SELECT * FROM dune.hdser.query_3601593
    WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),


gnosis_omen_markets_odds_reserves AS (
    SELECT 
        t1.* 
        ,t1.cumsum_feeAmount/POWER(10,t3.decimals) * t3.price AS cumsum_feeAmount_usd
    FROM query_3668140 t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        prices.usd t3
        ON t3.contract_address = t2.collateraltoken
        AND t3.blockchain = 'gnosis'
        AND t3.minute = DATE_TRUNC('minute',t1.block_time)
),


sparse_hour AS (
    SELECT 
        DATE_TRUNC(step, t1.block_time) AS block_datetime
        ,ARRAY_AGG(t1.tvl_usd ORDER BY t1.block_time DESC, t1.evt_index DESC)[1] AS tvl_usd
        ,ARRAY_AGG(t2.cumsum_feeAmount_usd ORDER BY t2.block_time DESC, t2.evt_index DESC)[1] AS cumsum_feeAmount_usd
    FROM 
        gnosis_omen_markets_tvl t1
    CROSS JOIN
        resolution
    LEFT JOIN
        gnosis_omen_markets_odds_reserves t2
        ON
        DATE_TRUNC(step, t2.block_time) = DATE_TRUNC(step, t1.block_time)
    GROUP BY 1
),

range AS (
    SELECT 
        CAST(MIN(block_datetime) AS TIMESTAMP) AS start_time
        ,LEAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST((SELECT opening_time FROM omen_gnosis_markets_status) AS TIMESTAMP)) AS end_time
    FROM
        sparse_hour
),

calendar_hour AS (
    SELECT 
        block_datetime 
    FROM 
        range
        ,UNNEST( sequence( 
            start_time, 
            end_time, 
            IF(DATE_DIFF('hour',start_time,end_time)>=10000, INTERVAL '1' day, INTERVAL '1' hour)
            )
        ) t(block_datetime)
),

dense AS (
    SELECT 
        t1.block_datetime
        ,t2.tvl_usd
        ,t2.cumsum_feeAmount_usd
    FROM 
        calendar_hour t1
    LEFT JOIN
        sparse_hour t2
        ON
        t2.block_datetime = t1.block_datetime
),

daily_tvl_per_marker AS (
    SELECT 
        block_datetime
        ,LAST_VALUE(tvl_usd) IGNORE NULLS OVER (ORDER BY block_datetime) AS tvl_usd
        ,LAST_VALUE(cumsum_feeAmount_usd) IGNORE NULLS OVER (ORDER BY block_datetime) AS cumsum_feeAmount_usd
    FROM
        dense
)


SELECT 
    block_datetime
    ,tvl_usd AS "Market Reserves"
    ,cumsum_feeAmount_usd AS "Pool Fees"
FROM
    daily_tvl_per_marker
ORDER BY 1 
