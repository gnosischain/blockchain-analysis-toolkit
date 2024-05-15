-- query_id: 3684914

WITH

ConditionalTokens_evt_TransferBatch AS (
    SELECT 
        evt_index
        ,evt_block_time
        ,"from"
        ,to
        ,id
        ,value
        ,idx - 1 AS outcomeIndex
    FROM
        omen_gnosis.ConditionalTokens_evt_TransferBatch
    CROSS JOIN UNNEST(ids, "values") WITH ORDINALITY AS a(id, value, idx)
),

ConditionalTokens_evt_TransferSingle AS (
    SELECT 
        evt_index
        ,evt_block_time
        ,"from"
        ,to
        ,id
        ,value
        ,NULL AS outcomeIndex
    FROM
        omen_gnosis.ConditionalTokens_evt_TransferSingle
),

OutcomeTokens_transfers AS (
    SELECT * FROM ConditionalTokens_evt_TransferBatch
    UNION ALL
    SELECT * FROM ConditionalTokens_evt_TransferSingle
),

inflow AS (
    SELECT
        id
        ,evt_block_time 
        ,evt_index
        ,to AS user
        ,CAST(value AS INT256) AS value
        ,outcomeIndex
    FROM 
        OutcomeTokens_transfers
),

outflow AS (
    SELECT
        id
        ,evt_block_time 
        ,evt_index
        ,"from" AS user
        ,CAST(-value AS INT256) AS value
        ,outcomeIndex
    FROM 
        OutcomeTokens_transfers
),

netflow AS (
    SELECT
        id
        ,evt_block_time
        ,evt_index
        ,user
        ,value AS outcomeToken
        ,MAX(outcomeIndex) OVER (PARTITION BY id) AS outcomeIndex
    FROM (
        SELECT * FROM inflow
        UNION ALL
        SELECT * FROM outflow
    )
),

omen_gnosis_markets AS (
    SELECT fixedProductMarketMaker, collateralToken, outcomeSlotCount FROM query_3668567
),

outputTokens_market AS (
    SELECT
        DISTINCT
        t2.fixedProductMarketMaker
        ,t2.collateralToken
        ,t2.outcomeSlotCount
        ,t1.id
    FROM 
        netflow t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.fixedProductMarketMaker = t1.user
),

netflow_market AS (
    SELECT 
        t2.fixedProductMarketMaker
        ,t2.collateralToken
        ,t2.outcomeSlotCount
        ,t1.* 
    FROM 
        netflow t1
    INNER JOIN 
        outputTokens_market t2
        ON t2.id = t1.id
),

outcomeToken_balances_delta AS (
    SELECT
        fixedProductMarketMaker 
        ,collateralToken
        ,evt_block_time
        ,evt_index
        ,user
        ,TRANSFORM(
            SEQUENCE(0,CAST(outcomeSlotCount AS INTEGER) - 1), 
            x -> IF(CARDINALITY(delta_outcomeTokens) > 1, delta_outcomeTokens[x+1], IF(outcomeIndexes[1] = x,delta_outcomeTokens[1],CAST(0 AS INT256) ))
        ) AS delta_outcomeTokens
    FROM (
        SELECT 
            fixedProductMarketMaker 
            ,collateralToken
            ,evt_block_time
            ,evt_index
            ,user
            ,outcomeSlotCount
           ,ARRAY_AGG(outcomeToken ORDER BY outcomeIndex) AS delta_outcomeTokens
           ,ARRAY_AGG(outcomeIndex ORDER BY outcomeIndex) AS outcomeIndexes
        FROM netflow_market
        GROUP BY 1,2,3,4,5,6
    )
)

SELECT
     fixedProductMarketMaker
     ,collateralToken
    ,evt_block_time
    ,evt_index
    ,user
    ,ARRAY_AGG(value ORDER BY idx) AS outcomeTokens
FROM (
    SELECT
        fixedProductMarketMaker
        ,collateralToken
        ,evt_block_time
        ,evt_index
        ,user
        ,idx
        ,SUM(value) OVER (PARTITION BY fixedProductMarketMaker, user, idx ORDER BY evt_block_time, evt_index) AS value
    FROM
        outcomeToken_balances_delta
        ,UNNEST(delta_outcomeTokens) WITH ORDINALITY d(value,idx)
)
GROUP BY
    1,2,3,4,5