/*
======= Query Info =======                     
-- query_id: 3725565                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=agent_traders, value=PredictionProphetGPT4, type=enum)]                     
-- last update: 2024-07-25 17:22:52.274403                     
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
        ,DATE_TRUNC('hour',t1.evt_block_time) AS date_time
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
    SELECT 
        fixedProductMarketMaker
        ,collateralToken
        ,CAST(MIN(date_time) AS TIMESTAMP) AS date_time_min
        ,CAST(MAX(date_time) AS TIMESTAMP) AS date_time_max
    FROM gnosis_omen_outcomeTokens_balance_sparse
    GROUP BY 1, 2
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
                    ELSE -CAST(value AS INT256)
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
             t1.evt_block_time >= t2.date_time_min
            AND
            (
                (
                    t2.fixedproductmarketmaker = t1."from"
                    AND
                    t3.address = t1.to
                )
                OR 
                (
                    t2.fixedproductmarketmaker = t1.to
                    AND
                    t3.address = t1."from"
                )
            )
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

omen_gnosis_markets_odds_reserves AS (
    SELECT 
        fixedproductmarketmaker
        ,DATE_TRUNC('hour', block_time) AS date_time 
       -- ,ARRAY_AGG(reserves ORDER BY block_time DESC)[1] AS reserves 
        ,ARRAY_AGG(odds ORDER BY block_time DESC)[1] AS odds 
    FROM
        query_3668140 
    INNER JOIN
        relevant_markets
        USING(fixedproductmarketmaker)
    GROUP BY
        1, 2
),

omen_gnosis_markets_odds_reserves_lead AS (
    SELECT 
        fixedproductmarketmaker
        ,date_time 
       -- ,reserves 
        ,odds 
        ,LEAD(date_time) OVER (PARTITION BY fixedproductmarketmaker ORDER BY date_time) AS date_time_lead
    FROM
        omen_gnosis_markets_odds_reserves 
),


calendar AS (
    SELECT
        fixedproductmarketmaker
        ,date_time
    FROM
        relevant_markets 
        ,UNNEST(SEQUENCE(date_time_min,date_time_max, INTERVAL '1' HOUR)) a(date_time)
),

odds_and_balance AS (
    SELECT 
        t1.fixedproductmarketmaker
        ,t3.collateralToken
        ,t1.date_time
        --,t2.reserves
        ,t2.odds
        ,t3.outcomeTokens
        ,ARRAY_MAX(t3.outcomeTokens) AS max_payout 
        ,ARRAY_MIN(t3.outcomeTokens) AS min_payout 
        ,REDUCE(
            ZIP_WITH(t2.odds,t3.outcomeTokens, (x,s) -> x*s)
            ,0
            ,(s, x) -> s + x, s -> s
        ) AS realizable
        ,t4.amount AS invested
    FROM
        calendar t1
    LEFT JOIN 
        omen_gnosis_markets_odds_reserves_lead t2 
        ON
        t2.fixedProductMarketMaker = t1.fixedProductMarketMaker
    LEFT JOIN
        gnosis_omen_outcomeTokens_balance_sparse_lead t3
        ON
        t3.fixedProductMarketMaker = t1.fixedProductMarketMaker
    LEFT JOIN
        colateral_invested t4
        ON
        t4.fixedProductMarketMaker = t1.fixedProductMarketMaker
    WHERE
        t1.date_time >= t2.date_time AND (t1.date_time < t2.date_time_lead OR t2.date_time_lead IS NULL) 
        AND
        t1.date_time >= t3.date_time AND (t1.date_time < t3.date_time_lead OR t3.date_time_lead IS NULL) 
        AND
        t1.date_time >= t4.date_time AND (t1.date_time < t4.date_time_lead OR t4.date_time_lead IS NULL)
),





ai_agents_tokens_balance AS (
    SELECT
        date_time
        ,COALESCE(balance_usd,0) AS balance_usd
        ,LEAD(date_time) OVER (ORDER BY date_time) AS date_time_lead
    FROM (
    SELECT
         t1.date_time
        ,SUM(t1.balance/POWER(10,t2.decimals)*t2.price) AS balance_usd
    FROM (
        SELECT
        date_time
        ,token_address
        ,(SUM(balance_diff) OVER (PARTITION BY token_address ORDER BY date_time)) AS balance
    FROM (
        SELECT 
            DATE_TRUNC('hour', evt_block_time) AS date_time
            ,token_address
            ,SUM(value) AS balance_diff
        FROM query_3715148 
        INNER JOIN
            ai_agents_traders 
            USING (address)
        GROUP BY
            1,2
        ) 
    ) t1
    LEFT JOIN
            prices.usd t2
            ON t2.contract_address = t1.token_address
            AND t2.blockchain = 'gnosis'
            AND t2.minute = t1.date_time
    GROUP BY 
        1
    )
),

PayoutRedemption AS (
    SELECT
        t1.date_time
        ,SUM(t1.payout/POWER(10,t2.decimals)*t2.price) AS payout_usd
    FROM (
    SELECT
        DATE_TRUNC('hour', t1.evt_block_time) AS date_time
        ,t1.collateralToken
        ,SUM(t1.payout) AS payout
    FROM
        omen_gnosis.ConditionalTokens_evt_PayoutRedemption t1
     INNER JOIN
            ai_agents_traders t2
            ON
            t2.address = t1.redeemer
    GROUP BY
        1, 2
    ) t1
    LEFT JOIN
            prices.usd t2
            ON t2.contract_address = t1.collateralToken
            AND t2.blockchain = 'gnosis'
            AND t2.minute = t1.date_time
    GROUP BY
        1
),

final AS (
    SELECT 
        t1.date_time
        ,t2.balance_usd AS "Balance"
        ,COALESCE(t4.payout_usd,0) AS "Payout"
        ,SUM((t1.max_payout + t1.invested)/POWER(10,t3.decimals)*t3.price) AS "Max Profit"
        ,SUM((t1.min_payout + t1.invested)/POWER(10,t3.decimals)*t3.price) AS "Min Profit"
        ,SUM(COALESCE((t1.realizable ),0)/POWER(10,t3.decimals)*t3.price) AS "Realizable"
        ,SUM((t1.invested)/POWER(10,t3.decimals)*t3.price) AS "Investment"
    FROM odds_and_balance t1
    LEFT JOIN
        ai_agents_tokens_balance t2
        ON TRUE
     LEFT JOIN
            prices.usd t3
            ON t3.contract_address = t1.collateralToken
            AND t3.blockchain = 'gnosis'
            AND t3.minute = t1.date_time
    LEFT JOIN
        PayoutRedemption t4
        ON
        t4.date_time = t1.date_time
    WHERE
        t1.date_time >= t2.date_time AND (t1.date_time < t2.date_time_lead OR t2.date_time_lead IS NULL) 
    GROUP BY 1, 2, 3
)

SELECT * FROM final