/*
======= Query Info =======                 
-- query_id: 3601593                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:43.173963                 
-- owner: hdser                 
==========================
*/

WITH

omen_gnosis_markets AS (
    SELECT * FROM query_3668567
),

LogNewAnswer  AS (
    SELECT * FROM omen_gnosis.Realitio_v2_1_evt_LogNewAnswer    
),

ConditionResolution  AS (
    SELECT * FROM omen_gnosis.ConditionalTokens_evt_ConditionResolution
),

final AS (
    SELECT
        question
        ,fixedProductMarketMaker
        ,fee
        ,conditionalTokens
        ,collateralToken
        ,questionId
        ,conditionId
        ,creation_time
        ,opening_time
        ,timeout
        ,answer_time
        ,answer
        ,resolution_time
        ,CASE
            WHEN NOW() < opening_time THEN 'Open'
            WHEN NOW() >= opening_time THEN
                CASE
                    WHEN answer_time[1] IS NULL THEN 'Closed'
                    WHEN NOW() < DATE_ADD('second',timeout,answer_time[1]) THEN 'Under Finalization'
                    WHEN NOW() >= DATE_ADD('second',timeout,answer_time[1]) AND resolution_time IS NULL THEN 'Finalized'
                    ELSE 'Resolved'
                END
        END AS status
        ,CASE
            WHEN answer[1] IS NULL THEN NULL
            WHEN answer[1] = 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff THEN False
            ELSE True
        END AS is_valid
        ,user
        ,bond
        ,history_hash
        ,is_commitment
        ,oracle
        ,payoutNumerators
        
    FROM (
    SELECT
        t1.question
        ,t1.fixedProductMarketMaker
        ,t1.fee
        ,t1.conditionalTokens
        ,t1.collateralToken
        ,t1.questionId
        ,t1.conditionId
        ,t1.creation_time
        ,t1.opening_time
        ,t1.timeout
        ,ARRAY_AGG(t2.evt_block_time ORDER BY t2.evt_block_time DESC) AS answer_time
        ,ARRAY_AGG(t2.answer ORDER BY t2.evt_block_time DESC) AS answer
        ,t3.evt_block_time AS resolution_time
        ,ARRAY_AGG(t2.user ORDER BY t2.evt_block_time DESC) AS user
        ,ARRAY_AGG(t2.bond ORDER BY t2.evt_block_time DESC) AS bond
        ,ARRAY_AGG(t2.history_hash ORDER BY t2.evt_block_time DESC) AS history_hash
        ,ARRAY_AGG(t2.is_commitment ORDER BY t2.evt_block_time DESC) AS is_commitment
        ,t3.oracle
        ,t3.payoutNumerators
    FROM
        omen_gnosis_markets t1
    LEFT JOIN
        LogNewAnswer t2
        ON
        t2.question_id = t1.questionId
    LEFT JOIN
        ConditionResolution t3
        ON 
        t3.questionId = t1.questionId
        AND
        t3.conditionId = t1.conditionId
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,13,18,19
    )
)

SELECT * FROM final
