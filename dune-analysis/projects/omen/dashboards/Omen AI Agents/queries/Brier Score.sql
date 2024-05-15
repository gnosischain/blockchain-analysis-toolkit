-- query_id: 3633110

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
        ,block_time
        ,action
        ,tx_hash
        ,evt_index
        ,reserves_delta
    FROM gnosis_reserves_liquidity
    UNION ALL
    SELECT
        fixedproductmarketmaker
        ,block_time
        ,action
        ,tx_hash
        ,evt_index
        ,reserves_delta
    FROM gnosis_reserves_trades
),

omen_gnosis_markets_status AS (
    SELECT * FROM query_3601593
    WHERE status = 'Resolved' AND is_valid = TRUE
),

omen_gnosis_markets AS (
    SELECT 
    t1.*, 
    FILTER(
          TRANSFORM(
            t2.payoutNumerators,
            x -> CASE WHEN x <> 0 THEN ARRAY_POSITION(t2.payoutNumerators, x) - 1 ELSE NULL END
          )
          , x -> x IS NOT NULL) AS payout_outcome,
    t2.resolution_time
    FROM query_3668567 t1
    INNER JOIN omen_gnosis_markets_status t2
    ON t2.fixedProductMarketMaker = t1.fixedProductMarketMaker
),

probabilities AS (
    SELECT 
        fixedproductmarketmaker
        ,block_time
        ,tx_hash
        ,evt_index
        ,TRANSFORM(
            reserves, 
            x -> ROUND( (CAST(1 AS REAL)/NULLIF(x,0)) / total_inv_reserves, 4)
        ) AS odds
    FROM (
        SELECT 
            fixedproductmarketmaker
            ,block_time
            ,tx_hash
            ,evt_index
            ,array_agg(reserve) AS reserves
            ,SUM(reserve) AS total_reserves
            ,SUM(CAST(1 AS REAL)/NULLIF(CAST(reserve AS REAL),0)) AS total_inv_reserves
        FROM (
            SELECT 
                fixedproductmarketmaker
                ,block_time
                ,action
                ,tx_hash
                ,evt_index
                ,idx
                ,SUM(value) OVER (PARTITION BY fixedproductmarketmaker, idx ORDER BY block_time, evt_index) AS reserve
            FROM 
                gnosis_reserves_delta 
            CROSS JOIN unnest(reserves_delta) WITH ORDINALITY AS a(value, idx)
        ) AS summed_values
        GROUP BY 
            1,2,3,4
    )
),

trades_list AS (
    SELECT
        t1.block_time
        ,t1.outcomeIndex AS outcome
        ,t1.amount/POWER(10,t5.decimals)*t5.price AS amount_usd
        ,t4.odds
        ,t3.fixedproductmarketmaker
    FROM
        gnosis_reserves_trades t1
    INNER JOIN
        omen_gnosis_markets t3
        ON t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
    INNER JOIN
        probabilities t4
        ON
        t4.tx_hash = t1.tx_hash
        AND
        t4.evt_index = t1.evt_index
    LEFT JOIN
        prices.usd t5
        ON t5.contract_address = t3.collateraltoken
        AND t5.blockchain = 'gnosis'
        AND t5.minute = DATE_TRUNC('minute',t1.block_time)
    WHERE
        t5.price IS NOT NULL
),

final AS (
    SELECT
        resolution_time
        ,fixedproductmarketmaker
        ,SUM(amount_usd) AS volume_usd
        ,COUNT(*) AS trades
        ,SUM(squared_error)/SUM(1) AS brier_score
    FROM (
        SELECT 
            t2.resolution_time
            ,t1.amount_usd
            ,t1.fixedproductmarketmaker
            ,t1.odds
            ,REDUCE(
                TRANSFORM(t1.odds, x -> CASE WHEN ARRAY_POSITION(t1.odds, x) = t2.payout_outcome[1] + 1 THEN x - 1 ELSE x END )
            , 0, (s, x) -> s + x*x, s -> s) AS squared_error 
            ,t2.payout_outcome
        FROM 
            trades_list t1
        INNER JOIN
            omen_gnosis_markets t2
            ON
            t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    )
    GROUP BY 1,2
)

SELECT
    day
    ,volume_usd
    ,trades
    ,brier_score_weighted_volume
    ,brier_score_weighted_trades
    ,AVG(brier_score_weighted_volume) OVER (ORDER BY day ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS brier_score_weighted_volume_avg
    ,AVG(brier_score_weighted_trades) OVER (ORDER BY day ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS brier_score_weighted_trades_avg
FROM (
    SELECT 
        DATE_TRUNC('day', resolution_time) AS day
        ,SUM(volume_usd) AS volume_usd
        ,SUM(trades) AS trades
        ,SUM(brier_score*volume_usd)/SUM(volume_usd)  AS brier_score_weighted_volume
        ,SUM(brier_score*trades)/SUM(trades)  AS brier_score_weighted_trades
    FROM 
        final
    GROUP BY 
        1
)
--WHERE day > DATE '2023-01-01'