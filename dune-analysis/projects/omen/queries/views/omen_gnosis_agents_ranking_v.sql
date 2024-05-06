-- query_id: 3644289

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
),
markets_outcome_final AS (
    SELECT 
        *
        ,array_union(payout_outcome,bet_outcome) = payout_outcome AS correct_bet
    FROM markets_outcome
),

final_all AS (
    SELECT 
        label
        ,COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) AS cnt_resolved
        ,COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS cnt_correct
        ,COUNT(CASE WHEN NOT correct_bet AND is_valid = True THEN 1 END) AS cnt_wrong
        ,CAST(COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS REAL) / COUNT(CASE WHEN market_status = 'Resolved' AND is_valid = True THEN 1 END) * 100 AS pct_correct
    FROM
        markets_outcome_final
    GROUP BY 1
),

final_last_month AS (
    SELECT 
        label
        ,COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) AS cnt_resolved
        ,COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS cnt_correct
        ,COUNT(CASE WHEN NOT correct_bet AND is_valid = True THEN 1 END) AS cnt_wrong
        ,CAST(COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS REAL) / COUNT(CASE WHEN market_status = 'Resolved' AND is_valid = True THEN 1 END) * 100 AS pct_correct
    FROM
        markets_outcome_final
    WHERE
        resolution_time >= NOW() - INTERVAL '1' MONTH
    GROUP BY 1
),

final_last_3month AS (
    SELECT 
        label
        ,COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) AS cnt_resolved
        ,COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS cnt_correct
        ,COUNT(CASE WHEN NOT correct_bet AND is_valid = True THEN 1 END) AS cnt_wrong
        ,CAST(COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS REAL) / COUNT(CASE WHEN market_status = 'Resolved' AND is_valid = True THEN 1 END) * 100 AS pct_correct
    FROM
        markets_outcome_final
    WHERE
        resolution_time >= NOW() - INTERVAL '3' MONTH
    GROUP BY 1
),

final_last_6month AS (
    SELECT 
        label
        ,COUNT(CASE WHEN market_status = 'Resolved' THEN 1 END) AS cnt_resolved
        ,COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS cnt_correct
        ,COUNT(CASE WHEN NOT correct_bet AND is_valid = True THEN 1 END) AS cnt_wrong
        ,CAST(COUNT(CASE WHEN correct_bet AND is_valid = True THEN 1 END) AS REAL) / COUNT(CASE WHEN market_status = 'Resolved' AND is_valid = True THEN 1 END) * 100 AS pct_correct
    FROM
        markets_outcome_final
    WHERE
        resolution_time >= NOW() - INTERVAL '6' MONTH
    GROUP BY 1
),

final AS (
    SELECT *, 'Full' AS range, TIMESTAMP '2019-01-01 00:00' AS date_cutoff FROM final_all
    UNION ALL
    SELECT *, 'Last Month' AS range, NOW() - INTERVAL '1' MONTH AS date_cutoff FROM final_last_month
    UNION ALL
    SELECT *, 'Last 3 Months' AS range, NOW() - INTERVAL '3' MONTH AS date_cutoff FROM final_last_3month
    UNION ALL
    SELECT *, 'Last 6 Months' AS range, NOW() - INTERVAL '6' MONTH AS date_cutoff FROM final_last_6month
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