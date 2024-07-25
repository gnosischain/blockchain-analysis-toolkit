/*
======= Query Info =======                     
-- query_id: 3723143                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.882129                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_omen_outcomeTokens_balance_sparse AS (
    SELECT
        fixedProductMarketMaker
        ,collateralToken
        ,DATE_TRUNC('hour',evt_block_time) AS date_time
        ,ARRAY_AGG(outcomeTokens ORDER BY evt_block_time DESC)[1] AS outcomeTokens
    FROM
        query_3684914 
    WHERE
        user = 0x2DD9f5678484C1F59F97eD334725858b938B4102
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
            ,DATE_TRUNC('hour',t1.evt_block_time) AS date_time
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
        WHERE
            t1.to = 0x2DD9f5678484C1F59F97eD334725858b938B4102
            OR
            t1."from" = 0x2DD9f5678484C1F59F97eD334725858b938B4102 
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
        ,DATE_TRUNC('hour', block_time) AS date_time 
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
  --   LEFT JOIN
--        prices.usd t3
 --       ON t3.contract_address = t2.collateralToken
  --      AND t3.blockchain = 'gnosis'
--        AND t3.minute = DATE_TRUNC('minute',t1.date_time)
    WHERE
        t1.date_time >= t2.date_time AND (t1.date_time < t2.date_time_lead OR t2.date_time_lead IS NULL) 
        AND
        t1.date_time >= t4.date_time AND (t1.date_time < t4.date_time_lead OR t4.date_time_lead IS NULL)
)

SELECT * FROM odds_and_balance

