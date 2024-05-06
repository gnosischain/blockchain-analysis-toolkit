-- query_id: 3669265

WITH

gnosis_omen_markets_tvl AS (
    SELECT * FROM dune.hdser.query_3668377
),

omen_gnosis_markets AS (
    SELECT * FROM dune.hdser.query_3668567
    --dune.hdser.result_omen_gnosis_markets_mv
),

gnosis_omen_markets_odds_reserves AS (
    SELECT 
        t1.* 
        ,t1.cumsum_feeAmount/POWER(10,t3.decimals) * t3.price AS cumsum_feeAmount_usd
    FROM dune.hdser.query_3668140 t1
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
        t1.fixedproductmarketmaker
        ,DATE_TRUNC('day', t1.block_time) AS block_datetime
        ,ARRAY_AGG(t1.tvl_usd ORDER BY t1.block_time DESC, t1.evt_index DESC)[1] AS tvl_usd
        ,ARRAY_AGG(t2.cumsum_feeAmount_usd ORDER BY t2.block_time DESC, t2.evt_index DESC)[1] AS cumsum_feeAmount_usd
    FROM 
        gnosis_omen_markets_tvl t1
    LEFT JOIN
        gnosis_omen_markets_odds_reserves t2
        ON
        t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
        AND
        DATE_TRUNC('day', t2.block_time) = DATE_TRUNC('day', t1.block_time)
    GROUP BY 1, 2
),

range AS (
    SELECT 
        fixedproductmarketmaker
        ,CAST(MIN(block_datetime) AS TIMESTAMP) AS start_time
        ,CAST(CURRENT_DATE AS TIMESTAMP) AS end_time
        --,CAST(MAX(block_datetime) AS TIMESTAMP) AS end_time
    FROM
        sparse_hour
    GROUP BY 1
),

calendar_hour AS (
    SELECT 
        fixedproductmarketmaker
        ,block_datetime 
    FROM 
        range
        ,UNNEST( sequence( start_time, end_time, INTERVAL '1' day)) t(block_datetime)
),

dense AS (
    SELECT 
        t1.fixedproductmarketmaker
        ,t1.block_datetime
        ,t2.tvl_usd
        ,t2.cumsum_feeAmount_usd
    FROM 
        calendar_hour t1
    LEFT JOIN
        sparse_hour t2
        ON
        t2.block_datetime = t1.block_datetime
        AND
        t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
),

daily_tvl_per_marker AS (
    SELECT 
        fixedproductmarketmaker
        ,block_datetime
        ,LAST_VALUE(tvl_usd) IGNORE NULLS OVER (PARTITION BY fixedproductmarketmaker ORDER BY block_datetime) AS tvl_usd
        ,LAST_VALUE(cumsum_feeAmount_usd) IGNORE NULLS OVER (PARTITION BY fixedproductmarketmaker ORDER BY block_datetime) AS cumsum_feeAmount_usd
    FROM
        dense
)

SELECT 
    block_datetime
    ,SUM(tvl_usd) AS "Market Reserves"
    ,SUM(cumsum_feeAmount_usd) AS "Pool Fees"
FROM
    daily_tvl_per_marker
GROUP BY 1
ORDER BY 1 