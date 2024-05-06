-- query_id: 3586635

WITH

omen_gnosis_trades AS (
      SELECT * FROM omen_gnosis.trades
),

omen_gnosis_markets_status AS (
    SELECT * FROM dune.hdser.query_3601593
),

omen_gnosis_markets AS (
    SELECT * FROM dune.hdser.result_omen_gnosis_markets_mv
),


ai_agents_traders AS (
    SELECT * FROM dune.hdser.query_3582994
    WHERE label = '{{agent_traders}}'
),

relevant_markets AS (
    SELECT
        fixedproductmarketmaker
        ,label
        ,ARRAY_AGG(DISTINCT bet_outcome ) AS bet_outcome
    FROM (
        SELECT DISTINCT 
            t2.label
            ,fixedproductmarketmaker
            ,outcomeIndex AS bet_outcome
        FROM omen_gnosis_trades t1
        INNER JOIN
            ai_agents_traders AS t2
            ON t2.address = t1.tx_from OR t2.address = t1.tx_to
        WHERE
            action = 'Buy'
    )
    GROUP BY 1, 2

),

markets_outcome AS (
SELECT 
        t2.label
        ,t1.conditionid
        ,t1.questionid
        ,t1.fixedproductmarketmaker
        ,t1.creation_time AS start_time
        ,t1.opening_time
        ,t1.timeout
        ,t3.Status AS market_status
        ,FILTER(
          TRANSFORM(
            t3.payoutNumerators,
            x -> CASE WHEN x <> 0 THEN ARRAY_POSITION(t3.payoutNumerators, x) - 1 ELSE NULL END
          )
          , x -> x IS NOT NULL) AS payout_outcome
        ,t2.bet_outcome
        ,t3.resolution_time
        ,t3.is_valid
    FROM 
        omen_gnosis_markets t1
    INNER JOIN
        relevant_markets t2
       ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        omen_gnosis_markets_status t3
        ON t3.conditionId = t1.conditionid
        AND t3.questionId = t1.questionid
   -- WHERE
    --    t3.is_valid = True
),
final AS (
    SELECT 
        *
        ,array_union(payout_outcome,bet_outcome) = payout_outcome AS correct_bet
    FROM markets_outcome
)

SELECT 
     COUNT(CASE WHEN market_status = 'Open' THEN 1 END) AS cnt_open
    ,COUNT(CASE WHEN market_status = 'Closed' THEN 1 END) AS cnt_closed
    ,COUNT(CASE WHEN market_status = 'Under Finalization' THEN 1 END) AS cnt_under_finalization
    ,COUNT(CASE WHEN market_status = 'Finalized' THEN 1 END) AS cnt_finalized
    ,COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) AS cnt_resolved
    ,-COUNT(CASE WHEN market_status = 'Resolved' AND is_valid = False THEN 1 END) AS cnt_resolved_fail
    ,COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS cnt_correct
    ,COUNT(CASE WHEN NOT correct_bet AND is_valid = True THEN 1 END) AS cnt_wrong
    ,CAST(COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS REAL) / NULLIF(COUNT(CASE WHEN market_status = 'Resolved' AND is_valid = True THEN 1 END),0) * 100 AS pct_correct
FROM
    final