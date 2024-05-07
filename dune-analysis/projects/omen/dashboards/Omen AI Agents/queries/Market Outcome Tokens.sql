-- query_id: 3685027

WITH

gnosis_omen_markets_odds_reserves AS (
    SELECT * FROM dune.hdser.query_3668140
    WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

omen_gnosis_markets_status AS (
    SELECT * FROM dune.hdser.query_3601593
    WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

sparse_prob AS (
    SELECT 
        hour
        ,value AS outcomeToken_odds
        ,idx - 1 AS outcomeIndex
    FROM (
         SELECT 
            DATE_TRUNC('hour', block_time) AS hour
            ,odds
            ,ROW_NUMBER() OVER (
                PARTITION BY
                    DATE_TRUNC('hour', block_time)
                ORDER BY 
                    block_time DESC
                    ,evt_index DESC
            ) as rn
        FROM gnosis_omen_markets_odds_reserves
    )
    ,UNNEST(odds) WITH ORDINALITY o(value,idx)
    WHERE
        rn = 1
),

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
            ,COALESCE(CAST((SELECT resolution_time FROM omen_gnosis_markets_status) AS TIMESTAMP),  CAST(MAX(hour) AS TIMESTAMP)) AS max_hour
            ,ARRAY_AGG(DISTINCT outcomeIndex) AS outcomesIndex
        FROM
            sparse_suply
    )
    ,UNNEST(SEQUENCE(min_hour, max_hour,INTERVAL '1' HOUR))  s(hour)
    ,UNNEST(outcomesIndex)  s(outcomeIndex)
),

final AS (
    SELECT
        hour
        ,CASE
            WHEN hour <= (SELECT opening_time FROM omen_gnosis_markets_status)
                THEN LAST_VALUE(outcomeToken_odds) IGNORE NULLS OVER (PARTITION BY outcomeIndex ORDER BY hour) 
            ELSE NULL 
            END AS outcomeToken_odds
        ,LAST_VALUE(outcomeToken) IGNORE NULLS OVER (PARTITION BY outcomeIndex ORDER BY hour) AS outcomeToken
        ,outcomeIndex
    FROM (
        SELECT 
            t1.hour
            ,t2.outcomeToken_odds
            ,t3.outcomeToken
            ,t1.outcomeIndex
        FROM dates_range t1
        LEFT JOIN
            sparse_prob t2
            ON 
            t2.hour = t1.hour
            AND
            t2.outcomeIndex = t1.outcomeIndex
        LEFT JOIN
            sparse_suply t3
            ON 
            t3.hour = t1.hour
            AND
            t3.outcomeIndex = t1.outcomeIndex
    )
)


SELECT * FROM final
