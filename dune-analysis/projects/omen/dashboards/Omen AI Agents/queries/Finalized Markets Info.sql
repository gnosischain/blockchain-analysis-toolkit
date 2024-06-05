-- query_id: 3782132

WITH

omen_gnosis_markets_status AS (
    SELECT * FROM query_3601593
    WHERE
        status IN ('Finalized','Under Finalization')
),

gnosis_omen_markets_tvl AS (
    SELECT 
        t1.fixedproductmarketmaker
        ,t3.symbol
        ,t3.decimals
        ,ARRAY_AGG(t1.tvl ORDER BY t1.block_time DESC, t1.evt_index DESC)[1] AS tvl
    FROM query_3668377 t1
    INNER JOIN
        omen_gnosis_markets_status t2
        ON 
        t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        tokens.erc20 t3
        ON
        t3.blockchain = 'gnosis'
        AND
        t3.contract_address = t2.collateralToken
    GROUP BY
        1, 2, 3
),

gnosis_omen_outcomeTokens_supply AS (
    --gnosis_omen_outcomeTokens_balance_sparse_v
    SELECT  
        t1.fixedproductmarketmaker
        ,-ARRAY_AGG(t1.outcomeTokens ORDER BY t1.evt_block_time DESC, t1.evt_index DESC)[1][1] AS supply
    FROM query_3684914 t1
    INNER JOIN
        omen_gnosis_markets_status t2
        ON 
        t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    WHERE 
        t1.user = 0x0000000000000000000000000000000000000000
    GROUP BY 
        1
),

gnosis_omen_markets_odds_reserves AS (
    SELECT 
        t1.fixedproductmarketmaker
        ,ARRAY_AGG(t1.odds ORDER BY t1.block_time DESC, t1.evt_index DESC)[1] AS odds
    FROM query_3668140 t1
    INNER JOIN
        omen_gnosis_markets_status t2
        ON 
        t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    GROUP BY 
        1
)

SELECT 
    status,
    REGEXP_REPLACE(SPLIT_PART(question, '␟', 3) ,'[^A-Za-z]', '') AS Category,
    ARRAY[SPLIT_PART(question, '␟', 1), SPLIT_PART(question, '␟', 2) ] AS Question,
    DATE_DIFF('day',creation_time, opening_time) AS "Duration in Days",
    opening_time AS "Closing Time",
    answer_time
FROM omen_gnosis_markets_status t1
INNER JOIN
    gnosis_omen_markets_tvl t2
    ON 
    t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
INNER JOIN
    gnosis_omen_outcomeTokens_supply t3
    ON 
    t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
INNER JOIN
    gnosis_omen_markets_odds_reserves t4
    ON 
    t4.fixedproductmarketmaker = t1.fixedproductmarketmaker
