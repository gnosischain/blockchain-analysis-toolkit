-- query_id: 3668140

WITH

gnosis_reserves_trades AS (
      SELECT * FROM omen_gnosis.trades
),

gnosis_reserves_liquidity AS (
    SELECT * FROM omen_gnosis.liquidity
),

gnosis_reserves_delta AS (
    SELECT
        fixedproductmarketmaker
        ,action
        ,block_time
        ,evt_index
        ,tx_hash
        ,COALESCE(CAST(-collateralRemovedFromFeePool AS INT256),0) AS feeAmount
        ,reserves_delta
    FROM gnosis_reserves_liquidity
    UNION ALL
    SELECT
        fixedproductmarketmaker
        ,action
        ,block_time
        ,evt_index
        ,tx_hash
        ,CAST(feeAmount AS INT256) AS feeAmount
        ,reserves_delta
    FROM gnosis_reserves_trades
),


reserves AS (
    SELECT 
        fixedproductmarketmaker
        ,action
        ,block_time
        ,evt_index
        ,tx_hash
        ,feeAmount
        ,SUM(feeAmount) OVER (PARTITION BY fixedproductmarketmaker ORDER BY block_time, evt_index) AS cumsum_feeAmount
        ,reserves
        ,TRANSFORM(
            reserves, 
            x -> (CAST(1 AS DOUBLE)/NULLIF(x,0)) / total_inv_reserves
        ) AS odds
    FROM (
        SELECT 
            fixedproductmarketmaker
            ,action
            ,block_time
            ,evt_index
            ,tx_hash
            ,feeAmount
            ,array_agg(reserve ORDER BY idx) AS reserves
            ,SUM(CAST(1 AS DOUBLE)/NULLIF(CAST(reserve AS DOUBLE),0)) AS total_inv_reserves
        FROM (
            SELECT 
                fixedproductmarketmaker
                ,action
                ,block_time
                ,evt_index
                ,tx_hash
                ,feeAmount
                ,idx
                ,SUM(value) OVER (PARTITION BY fixedproductmarketmaker, idx ORDER BY block_time, evt_index) AS reserve
            FROM 
                gnosis_reserves_delta 
            CROSS JOIN unnest(reserves_delta) WITH ORDINALITY AS a(value, idx)
        ) AS summed_values
        GROUP BY 
            1,2,3,4,5,6
    )
)

SELECT * FROM reserves