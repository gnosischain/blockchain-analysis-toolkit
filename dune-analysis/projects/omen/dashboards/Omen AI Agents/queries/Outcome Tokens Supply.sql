/*
======= Query Info =======                     
-- query_id: 3684902                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=fixedproductmarketmaker, value=0xc30a903231f2c7f6ad6997b75d0662053e775fc0, type=enum)]                     
-- last update: 2024-07-25 17:22:48.378340                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_omen_outcomeTokens_supply AS (
    --gnosis_omen_outcomeTokens_balance_sparse_v
    SELECT * FROM dune.hdser.query_3684914
    WHERE 
        user = 0x0000000000000000000000000000000000000000
        AND
        fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

sparse_suply AS (
    SELECT
        hour
        ,CAST(-outcomeToken AS DOUBLE)/1e18 AS outcomeToken
        ,idx - 1 AS outcomeIndex
    FROM (
        SELECT 
            DATE_TRUNC('hour', evt_block_time) AS hour
            ,outcomeTokens
            ,ROW_NUMBER() OVER (
                PARTITION BY
                    DATE_TRUNC('hour', evt_block_time)
                ORDER BY 
                    evt_block_time DESC
                    ,evt_index DESC
            ) as rn
        FROM gnosis_omen_outcomeTokens_supply
    )
    ,UNNEST(outcomeTokens) WITH ORDINALITY o(outcomeToken,idx)
    WHERE rn=1
),

dates_range AS (
    SELECT
        hour
        ,outcomeIndex
    FROM ( 
        SELECT 
            CAST(MIN(hour) AS TIMESTAMP) AS min_hour
            ,CAST(MAX(hour) AS TIMESTAMP) AS max_hour
            ,ARRAY_AGG(DISTINCT outcomeIndex) AS outcomesIndex
        FROM
            sparse_suply
    )
    ,UNNEST(SEQUENCE(min_hour, max_hour,INTERVAL '1' HOUR))  s(hour)
    ,UNNEST(outcomesIndex)  s(outcomeIndex)
)

SELECT
    hour
    ,LAST_VALUE(outcomeToken) IGNORE NULLS OVER (PARTITION BY outcomeIndex ORDER BY hour) AS outcomeToken
    ,outcomeIndex
FROM (
    SELECT 
        t1.hour
        ,t2.outcomeToken
        ,t1.outcomeIndex
    FROM dates_range t1
    LEFT JOIN
        sparse_suply t2
        ON 
        t2.hour = t1.hour
        AND
        t2.outcomeIndex = t1.outcomeIndex
)