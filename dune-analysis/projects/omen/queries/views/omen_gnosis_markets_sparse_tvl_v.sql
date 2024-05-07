-- query_id: 3668377

WITH

omen_gnosis_markets_odds_reserves AS (
    SELECT 
        fixedproductmarketmaker
        ,action
        ,block_time
        ,evt_index
        ,tx_hash
        ,feeAmount
        ,cumsum_feeAmount
        ,reserves
        ,odds
    FROM query_3668140
),

omen_gnosis_markets AS (
    SELECT * FROM dune.hdser.query_3668567
),

markets_tvl AS (
    SELECT 
        t1.fixedproductmarketmaker
        ,t1.block_time
        ,t1.evt_index
        ,ARRAY_MIN(t1.reserves)/POWER(10,t3.decimals) AS tvl
        ,ARRAY_MIN(t1.reserves)/POWER(10,t3.decimals) * t3.price AS tvl_usd
    FROM 
        omen_gnosis_markets_odds_reserves t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        prices.usd t3
        ON t3.contract_address = t2.collateraltoken
        AND t3.blockchain = 'gnosis'
        AND t3.minute = DATE_TRUNC('minute',t1.block_time)
)

SELECT * FROM markets_tvl