-- query_id: 3608098

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
        t1.category
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
    WHERE
        t3.is_valid = True
),

final AS (
    SELECT 
        *
        ,array_union(payout_outcome,bet_outcome) = payout_outcome AS correct_bet
    FROM markets_outcome
),

accuracy_sparse AS (
    SELECT 
        category
        ,resolution_time
        ,COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) OVER w AS cnt_resolved
        ,COUNT(CASE WHEN correct_bet THEN 1 END) OVER w AS cnt_correct
        ,CAST(COUNT(CASE WHEN correct_bet THEN 1 END) OVER w AS REAL) 
            / (COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) OVER w)  AS pct_correct
    FROM
        final
    WHERE 
        market_status = 'Resolved'
    WINDOW w AS (
        PARTITION BY 
            category
        ORDER BY 
            resolution_time
    )
),

start_times AS (
    SELECT
        category
        ,MIN(resolution_time) AS resolution_time_min
    FROM
        accuracy_sparse
    GROUP BY 
        1
),

calendar AS (
    SELECT
        t2.category
        ,DATE_TRUNC('hour',t1.time) AS hour
    FROM
        gnosis.blocks t1
    CROSS JOIN
        start_times t2
    WHERE
        t1.time >= t2.resolution_time_min
    GROUP BY 
        1,2
),

accuracy_sparse_hour AS (
    SELECT
        category
        ,DATE_TRUNC('hour', resolution_time) AS hour
         ,ARRAY_AGG(cnt_resolved ORDER BY resolution_time DESC)[1] AS cnt_resolved
         ,ARRAY_AGG(cnt_correct ORDER BY resolution_time DESC)[1] AS cnt_correct
        ,ARRAY_AGG(pct_correct ORDER BY resolution_time DESC)[1] AS pct_correct
    FROM
        accuracy_sparse 
    GROUP BY
        1, 2
)


SELECT 
    category
    ,hour
    ,LAST_VALUE(cnt_resolved) IGNORE NULLS OVER (PARTITION BY category ORDER BY hour) AS cnt_resolved
    ,LAST_VALUE(cnt_correct) IGNORE NULLS OVER (PARTITION BY category ORDER BY hour) AS cnt_correct
    ,LAST_VALUE(pct_correct) IGNORE NULLS OVER (PARTITION BY category ORDER BY hour) AS pct_correct
FROM (
    SELECT
        t2.category
        ,t2.hour
        ,t1.cnt_resolved
        ,t1.cnt_correct
        ,t1.pct_correct
    FROM
        accuracy_sparse_hour t1
    RIGHT JOIN
        calendar t2
        ON t2.category = t1.category
        AND t2.hour = t1.hour
)
ORDER BY 2