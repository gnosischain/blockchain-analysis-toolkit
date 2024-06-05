-- query_id: 3615668

WITH
omen_gnosis_trades AS (
      SELECT * FROM omen_gnosis.trades
),

omen_gnosis_markets_status AS (
    SELECT * FROM query_3601593
),

omen_gnosis_markets AS (
    SELECT * FROM query_3668567 
),

ai_agents_traders AS (
    SELECT * FROM query_3582994
),

trades_list AS (
    SELECT
         ARRAY_AGG(t3.question)AS question
        ,t3.fixedproductmarketmaker
        ,ARRAY_AGG(t1.action ORDER BY t1.block_time) AS actions
        ,ARRAY_AGG(t1.outcomeIndex ORDER BY t1.block_time) AS outcomes
        ,t4.status
        ,t4.is_valid
        ,FILTER(
          TRANSFORM(
            t4.payoutNumerators,
            x -> CASE WHEN x <> 0 THEN ARRAY_POSITION(t4.payoutNumerators, x) - 1 ELSE NULL END
          )
          , x -> x IS NOT NULL) AS payout_outcome
    FROM
        omen_gnosis_trades t1
    INNER JOIN
        ai_agents_traders AS t2
        ON t2.address = t1.tx_from OR t2.address = t1.tx_to
    INNER JOIN
        omen_gnosis_markets t3
        ON t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        omen_gnosis_markets_status t4
        ON t4.conditionId = t3.conditionid
        AND t4.questionId = t3.questionid
    WHERE
        t2.label = '{{agent_traders}}'
    GROUP BY
        2,5,6,7
)

SELECT 
    question
    ,actions
    ,outcomes
    ,status
    ,is_valid
    ,payout_outcome
    ,fixedproductmarketmaker
FROM trades_list