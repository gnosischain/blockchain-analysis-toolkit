-- query_id: 3644289

WITH

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
        FROM omen_gnosis.trades t1
        INNER JOIN
            query_3582994 AS t2 --ai_agents_traders
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
        query_3668567 t1 --omen_gnosis_markets
    INNER JOIN
        relevant_markets t2
       ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        query_3601593 t3 --omen_gnosis_markets_status
        ON t3.conditionId = t1.conditionid
        AND t3.questionId = t1.questionid
),

markets_outcome_final AS (
SELECT 
    label
    ,market_status
    ,resolution_time
    ,is_valid
    ,array_union(payout_outcome,bet_outcome) = payout_outcome AS correct_bet
FROM markets_outcome
),


range_timestamp AS (
    SELECT
        range
        ,date_cutoff
    FROM
        UNNEST(ARRAY[TIMESTAMP '2019-01-01 00:00',NOW() - INTERVAL '1' MONTH, NOW() - INTERVAL '3' MONTH, NOW() - INTERVAL '6' MONTH]) WITH ORDINALITY t(date_cutoff,idx)
        ,UNNEST(ARRAY['Full','Last Month','Last 3 Months', 'Last 6 Months']) WITH ORDINALITY s(range,idx)
    WHERE
        t.idx = s.idx
),

final AS (
    SELECT 
        t1.label
        ,t2.range
        ,t2.date_cutoff 
        ,COUNT(CASE WHEN t1.market_status = 'Resolved' THEN 1 END) AS cnt_resolved
        ,COUNT(CASE WHEN t1.correct_bet AND t1.is_valid = True THEN 1 END) AS cnt_correct
        ,COUNT(CASE WHEN NOT t1.correct_bet AND t1.is_valid = True THEN 1 END) AS cnt_wrong
        ,CAST(COUNT(CASE WHEN t1.correct_bet AND t1.is_valid = True THEN 1 END) AS REAL) / NULLIF(COUNT(CASE WHEN t1.market_status = 'Resolved' AND t1.is_valid = True THEN 1 END),0) * 100 AS pct_correct
    FROM
        markets_outcome_final t1
    INNER JOIN
        range_timestamp t2
        ON
        t1.resolution_time >= t2.date_cutoff
    GROUP BY 1,2,3
),

top10_resolved AS (
    SELECT
        label
        ,ranking
        ,range
        ,date_cutoff
    FROM (
        SELECT 
            ARRAY_AGG(label ORDER BY cnt_resolved DESC, pct_correct DESC) AS labels
            ,'Top 10 by Resolved Cnt' AS ranking
            ,range 
            ,date_cutoff
        FROM final
        GROUP BY range, date_cutoff
    ),
    UNNEST(labels) WITH ORDINALITY l(label,idx)
    WHERE 
        idx <= 10
),

top10_correct AS (
    SELECT
        label
        ,ranking
        ,range
        ,date_cutoff
    FROM (
        SELECT 
            ARRAY_AGG(label ORDER BY cnt_correct DESC, pct_correct DESC) AS labels
            ,'Top 10 by Correct Cnt' AS ranking
            ,range 
            ,date_cutoff
        FROM final
        GROUP BY range,date_cutoff
    ),
    UNNEST(labels) WITH ORDINALITY l(label,idx)
    WHERE 
        idx <= 10
),

top10_accuracy AS (
    SELECT
        label
        ,ranking
        ,range
        ,date_cutoff
    FROM (
        SELECT 
            ARRAY_AGG(label ORDER BY pct_correct DESC, cnt_resolved DESC, cnt_correct DESC) AS labels
            ,'Top 10 by Accuracy' AS ranking
            ,range 
            ,date_cutoff
        FROM final
        WHERE cnt_resolved >= 20
        GROUP BY range,date_cutoff
    ),
    UNNEST(labels) WITH ORDINALITY l(label,idx)
    WHERE 
        idx <= 10
),

pma_agents AS (
    SELECT
        label
        ,ranking
        ,range
        ,date_cutoff
    FROM (
        SELECT 
            ARRAY_AGG(label) AS labels
            ,'PMA' AS ranking
            ,range 
            ,date_cutoff
        FROM final
        WHERE label NOT LIKE 'Olas%' AND label NOT LIKE 'mech_%'
        GROUP BY range,date_cutoff
    ),
    UNNEST(labels) WITH ORDINALITY l(label,idx)
),

mechs_agents AS (
    SELECT
        label
        ,ranking
        ,range
        ,date_cutoff
    FROM (
        SELECT 
            ARRAY_AGG(label) AS labels
            ,'Mechs' AS ranking
            ,range 
            ,date_cutoff
        FROM final
        WHERE label LIKE 'mech_%'
        GROUP BY range,date_cutoff
    ),
    UNNEST(labels) WITH ORDINALITY l(label,idx)
)
    
SELECT * FROM top10_resolved
UNION ALL
SELECT * FROM top10_correct
UNION ALL
SELECT * FROM top10_accuracy
UNION ALL
SELECT * FROM pma_agents
UNION ALL 
SELECT * FROM mechs_agents
ORDER BY 2 ASC
