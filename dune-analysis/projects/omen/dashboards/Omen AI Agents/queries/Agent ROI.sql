/*
======= Query Info =======                     
-- query_id: 3870996                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=agent_traders, value=PredictionProphetGPT4, type=enum)]                     
-- last update: 2024-07-25 17:22:52.997723                     
-- owner: hdser                     
==========================
*/

WITH

ai_agents_traders AS (
    SELECT * FROM query_3582994
    WHERE label = '{{agent_traders}}'
),

relevant_markets AS (
    SELECT 
        t1.fixedProductMarketMaker
        ,t1.collateralToken
        ,CAST(MIN(evt_block_time) AS TIMESTAMP) AS date_time_min
        ,CAST(MAX(evt_block_time) AS TIMESTAMP) AS date_time_max
    FROM
        query_3684914 t1 --gnosis_omen_outcomeTokens_balance_sparse_v
    INNER JOIN
        ai_agents_traders t2
        ON
        t2.address = t1.user
    GROUP BY 1,2
),

market_investment AS (
        SELECT
            t2.fixedproductmarketmaker
            ,SUM(
                CASE 
                    WHEN t1."from" = t2.fixedproductmarketmaker THEN CAST(amount_raw AS INT256)
                    ELSE -CAST(amount_raw AS INT256)
                END
            ) AS invested
        FROM
            --test_schema.git_dunesql_1e47e2a_tokens_gnosis_transfers t1
            tokens_gnosis.transfers t1
        INNER JOIN
            relevant_markets t2
            ON
            t1.contract_address = t2.collateralToken
        CROSS JOIN
            ai_agents_traders t3
        WHERE
             t1.token_standard = 'erc20'
             AND
             t1.block_time >= t2.date_time_min
             AND
             t1.block_time <= t2.date_time_max
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
            1
),


PayoutRedemption AS (
    SELECT
        DATE_TRUNC('hour', t1.evt_block_time) AS date_time
        ,t3.fixedProductMarketMaker
        ,t1.collateralToken
        ,SUM(t1.payout) AS payout
    FROM
        omen_gnosis.ConditionalTokens_evt_PayoutRedemption t1
    INNER JOIN
            ai_agents_traders t2
            ON
            t2.address = t1.redeemer
    INNER JOIN
        query_3668567 t3 --omen_gnosis_markets_v
        ON
        t3.conditionId = t1.conditionId
    GROUP BY
        1, 2, 3
),


final AS (
    SELECT 
        t2.date_time
        ,t2.fixedproductmarketmaker
        ,t1.invested
        ,t2.payout
        ,COALESCE(t2.payout/POWER(10,18)/NULLIF(ABS(t1.invested)/POWER(10,18),0),0) - 1 AS roi
    FROM
        market_investment t1
    INNER JOIN 
        PayoutRedemption t2
        ON
        t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
)


SELECT
    day
    ,ROI_7DMA
    ,ROI_30DMA
    ,POWER(1 + ROI_7DMA, 52) - 1 AS AR_7DMA
    ,POWER(1 + ROI_30DMA, 12) - 1 AS AR_30DMA
    ,(POWER(1 + ROI_7DMA, 52) - 1) * 100 AS pct_AR_7DMA
    ,(POWER(1 + ROI_30DMA, 12) - 1) * 100 AS pct_AR_30DMA
FROM (
    SELECT
        day
        ,(AVG(avg_daily_roi) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS ROI_7DMA
        ,AVG(avg_daily_roi) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ROI_30DMA
    FROM (
        SELECT 
            DATE_TRUNC('day',date_time) AS day
            ,AVG(roi) AS avg_daily_roi
        FROM 
            final
        GROUP BY 1
    )
)
ORDER BY 1 DESC