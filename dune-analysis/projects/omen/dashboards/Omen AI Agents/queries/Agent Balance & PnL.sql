/*
======= Query Info =======                     
-- query_id: 3816937                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=agent_traders, value=PredictionProphetGPT4, type=enum)]                     
-- last update: 2024-07-25 17:22:57.199094                     
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
            ,DATE_TRUNC('hour',t1.block_time) AS date_time
            ,SUM(
                CASE 
                    WHEN t1."from" = t2.fixedproductmarketmaker THEN CAST(amount_raw AS INT256)
                    ELSE -CAST(amount_raw AS INT256)
                END
            ) AS amount
        FROM
            --query_3935887 t1 --tokens_gnosis_transfers_bare_v ASSUMPTION: not validator, no suicide
            test_schema.git_dunesql_8b40f15_tokens_gnosis_transfers t1
           -- tokens_gnosis.transfers t1
        INNER JOIN
            relevant_markets t2
            ON
            t1.contract_address = t2.collateralToken
        CROSS JOIN
            ai_agents_traders t3
        WHERE
             t1.block_time >= t2.date_time_min
             AND
             t1.token_standard = 'erc20'
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
        --,COALESCE(balance_usd,0) AS balance_usd
        ,balance_usd
        ,LEAD(date_time) OVER (ORDER BY date_time) AS date_time_lead
    FROM (
        SELECT
        date_time
        ,(SUM(balance_diff) OVER (ORDER BY date_time)) AS balance_usd
    FROM (
        SELECT 
            t1.block_hour AS date_time
            ,SUM(t1.amount_raw/POWER(10,decimals)*price) AS balance_diff
        FROM 
            query_3817332 t1 --gnosis_omen_ai_agents_balance_diff_hourly_sparse_v
        INNER JOIN
            ai_agents_traders 
            USING (address)
        LEFT JOIN
            prices.usd t2
            ON t2.blockchain = 'gnosis'
            AND t2.minute = t1.block_hour
            AND
            (
                t2.contract_address = t1.token_address
                OR 
                (
                    t2.contract_address = 0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d 
                    AND 
                    t1.token_standard = 'native'
                )
            )
        GROUP BY 1
        ) 
    ) t1
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
),

calendar_time AS (
    SELECT 
        date_time
    FROM (
        SELECT 
            MIN(date_time) AS min_date_time
            ,MAX(date_time) AS max_date_time
        FROM
            final
    ),
    UNNEST(SEQUENCE(min_date_time, max_date_time, INTERVAL '1' HOUR)) s(date_time)
)

SELECT 
    t2.date_time
    ,LAST_VALUE("Balance") IGNORE NULLS OVER (ORDER BY t2.date_time) AS "Balance"
    ,COALESCE("Payout",0) AS "Payout"
    ,LAST_VALUE("Max Profit") IGNORE NULLS OVER (ORDER BY t2.date_time) AS "Max Profit"
    ,LAST_VALUE("Min Profit") IGNORE NULLS OVER (ORDER BY t2.date_time) AS "Min Profit"
    ,LAST_VALUE("Realizable") IGNORE NULLS OVER (ORDER BY t2.date_time) AS "Realizable"
    ,LAST_VALUE("Investment") IGNORE NULLS OVER (ORDER BY t2.date_time) AS "Investment"
FROM final t1
RIGHT JOIN
    calendar_time t2
    ON 
    t2.date_time = t1.date_time