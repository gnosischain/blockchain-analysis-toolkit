-- query_id: 3578780

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

omen_gnosis_markets AS (
    SELECT * FROM query_3668567
),

ai_agents_traders AS (
    SELECT * FROM query_3582994
),

probabilities AS (
    SELECT 
        fixedproductmarketmaker
        ,block_time
        ,action
        ,tx_hash
        ,evt_index
        ,reserves
        ,total_reserves
        ,TRANSFORM(
            reserves, 
            x -> ROUND( (CAST(1 AS REAL)/NULLIF(x,0)) / total_inv_reserves, 4)
        ) AS odds
    FROM (
        SELECT 
            fixedproductmarketmaker
            ,block_time
            ,action
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
            1,2,3,4,5
    )
),

lag_probabilities AS (
SELECT
    fixedproductmarketmaker
    ,tx_hash
    ,evt_index
    ,odds
    ,LAG(odds) OVER (PARTITION BY fixedproductmarketmaker ORDER BY block_time, evt_index) AS odds_before
FROM
    probabilities
),

trades_list AS (
    SELECT
        t1.block_time
        ,get_href(get_chain_explorer_tx_hash('gnosis', t1.tx_hash), t1.action) AS action
        ,t1.outcomeIndex AS outcome
        ,t1.amount
        ,t4.odds_before
        ,t4.odds AS odds_after
        ,t3.fixedproductmarketmaker
    FROM
        gnosis_reserves_trades t1
    INNER JOIN
        ai_agents_traders AS t2
        ON t2.address = t1.tx_from OR t2.address = t1.tx_to
    INNER JOIN
        omen_gnosis_markets t3
        ON t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
    INNER JOIN
        lag_probabilities t4
        ON
        t4.tx_hash = t1.tx_hash
        AND
        t4.evt_index = t1.evt_index
    WHERE
        t2.label = '{{agent_traders}}'
)

SELECT * FROM trades_list