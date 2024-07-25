/*
======= Query Info =======                 
-- query_id: 3644396                 
-- description: ""                 
-- tags: []                 
-- parameters: [Parameter(name=market creator, value=Replicator, type=enum), Parameter(name=range, value=Last Month, type=enum), Parameter(name=ranking, value=Top 10 by Accuracy, type=enum)]                 
-- last update: 2024-07-25 17:22:44.635449                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_reserves_trades AS (
      SELECT * FROM omen_gnosis.trades
),

gnosis_reserves_liquidity AS (
    SELECT * FROM omen_gnosis.liquidity
),

ai_agents_makers AS (
    SELECT * FROM query_3584116
    WHERE
        label = '{{market creator}}' OR '{{market creator}}' = 'All'
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
          , x -> x IS NOT NULL) AS payout_outcome
    FROM query_3668567 t1 --omen_gnosis_markets
    INNER JOIN omen_gnosis_markets_status t2
    ON t2.fixedProductMarketMaker = t1.fixedProductMarketMaker
    INNER JOIN
        ai_agents_makers t3
        ON t3.address = t1.creator OR '{{market creator}}' = 'All'
),

ai_agents_traders AS (
    SELECT 
        t1.address
        ,t1.label
        ,t2.date_cutoff
    FROM 
         query_3582994 t1
    INNER JOIN
         query_3644289 t2
         ON t2.label = t1.label
    WHERE
        t2.ranking = '{{ranking}}'
        AND
        t2.range = '{{range}}'
),

probabilities AS (
    SELECT 
        fixedproductmarketmaker
        ,block_time
        ,action
        ,tx_hash
        ,evt_index
        ,reserves
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
        t2.label
        ,t1.fixedproductmarketmaker
        ,t1.block_time
        ,t1.outcomeIndex AS outcome
        ,t1.amount
        ,t4.odds_before
        ,t3.payout_outcome
    FROM
        gnosis_reserves_trades t1
    INNER JOIN
        ai_agents_traders AS t2
        ON t2.address = t1.tx_from OR  t2.address = t1.tx_to
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
        action = 'Buy'
        AND
        t1.block_time >= t2.date_cutoff 
        --AND t1.outcomeIndex = 0
),

odds_bin AS (
    SELECT 
        bin AS lbin
        ,bin+10 AS ubin
    FROM
        UNNEST(SEQUENCE(0, 90, 10)) t(bin)

),

avg_odds AS (
    SELECT
        t2.lbin
        ,t1.label
        ,t1.avg_bet_odds
        ,t1.outcome
        ,t1.amount
        ,t1.payout_outcome
    FROM (
        SELECT 
            label
            ,fixedproductmarketmaker
            ,outcome
            ,amount
            ,payout_outcome[1] AS payout_outcome
            ,AVG(odds_before[CAST(outcome AS INTEGER)+1]) AS avg_bet_odds
        FROM
            trades_list
        GROUP BY 1,2,3,4,5
    ) t1
    CROSS JOIN
        odds_bin t2
    WHERE
        t2.lbin<=t1.avg_bet_odds * 100 AND t2.ubin>t1.avg_bet_odds * 100
)

SELECT 
    t1.label
    ,t1.lbin/100.0 AS prob_bet
    ,t2.lbin/100.0 AS prob_res
    ,t1.cnt
    ,t1.volume
FROM (
    SELECT 
        label
        ,lbin
        ,CAST(SUM(CASE WHEN outcome = payout_outcome THEN amount ELSE 0 END) AS REAL)/POWER(10,18) AS volume
        ,SUM(CASE WHEN outcome = payout_outcome THEN 1 ELSE 0 END) AS cnt
        ,ROUND(AVG(CASE WHEN outcome = payout_outcome THEN 1 ELSE 0 END)*100) AS res_prob
    FROM avg_odds 
    GROUP BY 1,2
) t1
CROSS JOIN odds_bin t2
WHERE t2.lbin<=t1.res_prob  AND t2.ubin>t1.res_prob
ORDER BY label