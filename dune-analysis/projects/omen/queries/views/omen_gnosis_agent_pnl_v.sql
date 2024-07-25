/*
======= Query Info =======                     
-- query_id: 3725549                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=agent_traders, value=knownoutcomeagent, type=enum)]                     
-- last update: 2024-07-25 17:22:51.869776                     
-- owner: hdser                     
==========================
*/

WITH

ai_agents_traders AS (
    SELECT * FROM query_3582994
    WHERE label = '{{agent_traders}}'
),

gnosis_omen_outcomeTokens_balance_sparse AS (
    SELECT
        t1.fixedProductMarketMaker
        ,t1.collateralToken
        ,DATE_TRUNC('day',t1.evt_block_time) AS date_time
        ,ARRAY_AGG(t1.outcomeTokens ORDER BY t1.evt_block_time DESC)[1] AS outcomeTokens
    FROM
        query_3684914 t1
    INNER JOIN
        ai_agents_traders t2
        ON
        t2.address = t1.user
    GROUP BY 
        1,2,3
),

relevant_markets AS (
    SELECT DISTINCT
        fixedProductMarketMaker
        ,collateralToken
    FROM gnosis_omen_outcomeTokens_balance_sparse
),

colateral_invested AS (
    SELECT
        fixedproductmarketmaker
        ,collateralToken
        ,date_time
        ,SUM(amount) OVER (PARTITION BY fixedproductmarketmaker ORDER BY date_time) AS amount
        ,LEAD(date_time) OVER (PARTITION BY fixedproductmarketmaker ORDER BY date_time) AS date_time_lead
    FROM (
        SELECT
            t2.fixedproductmarketmaker
            ,t2.collateralToken
            ,DATE_TRUNC('day',t1.evt_block_time) AS date_time
            ,SUM(
                CASE 
                    WHEN t1."from" = t2.fixedproductmarketmaker THEN CAST(value AS INT256)
                    WHEN t1.to = t2.fixedproductmarketmaker THEN -CAST(value AS INT256)
                    ELSE 0
                END
            ) AS amount
        FROM
            erc20_gnosis.evt_transfer t1
        INNER JOIN
            relevant_markets t2
            ON
            t1.contract_address = t2.collateralToken
        CROSS JOIN
            ai_agents_traders t3
        WHERE
            t3.address = t1.to
            OR 
            t3.address = t1."from"
        GROUP BY
            1, 2, 3
    ) 
),

gnosis_omen_outcomeTokens_balance_sparse_lead AS (
    SELECT
        fixedProductMarketMaker
        ,collateralToken
        ,date_time
        ,outcomeTokens
        ,LEAD(date_time) OVER (PARTITION BY fixedProductMarketMaker ORDER BY date_time) AS date_time_lead
    FROM
        gnosis_omen_outcomeTokens_balance_sparse
),

revelant_markets AS (
    SELECT DISTINCT
        fixedProductMarketMaker
    FROM
        gnosis_omen_outcomeTokens_balance_sparse
),

omen_gnosis_markets_odds_reserves AS (
    SELECT 
        fixedproductmarketmaker
        ,DATE_TRUNC('day', block_time) AS date_time 
        ,ARRAY_AGG(odds ORDER BY block_time DESC)[1] AS odds 
    FROM
        query_3668140 
    INNER JOIN
        revelant_markets
        USING(fixedproductmarketmaker)
    GROUP BY
        1, 2
),

odds_and_balance AS (
    SELECT 
        t1.fixedproductmarketmaker
        ,t2.collateralToken
        ,t1.date_time
        ,t1.odds
        ,t2.outcomeTokens
        ,ARRAY_MAX(t2.outcomeTokens) AS max_payout --/POWER(10,t3.decimals) * t3.price AS max_payout_usd
        ,ARRAY_MIN(t2.outcomeTokens) AS min_payout --/POWER(10,t3.decimals) * t3.price AS min_payout_usd
        ,REDUCE(
            ZIP_WITH(t1.odds,t2.outcomeTokens, (x,s) -> x*s)
            ,0
            ,(s, x) -> s + x, s -> s
        ) AS realizable
        ,t4.amount AS invested
    FROM 
        omen_gnosis_markets_odds_reserves t1 
    LEFT JOIN
        gnosis_omen_outcomeTokens_balance_sparse_lead t2
        ON
        t2.fixedProductMarketMaker = t1.fixedProductMarketMaker
    LEFT JOIN
        colateral_invested t4
        ON
        t4.fixedProductMarketMaker = t1.fixedProductMarketMaker
    WHERE
        t1.date_time >= t2.date_time AND (t1.date_time < t2.date_time_lead OR t2.date_time_lead IS NULL) 
        AND
        t1.date_time >= t4.date_time AND (t1.date_time < t4.date_time_lead OR t4.date_time_lead IS NULL)
)

SELECT * FROM odds_and_balance
