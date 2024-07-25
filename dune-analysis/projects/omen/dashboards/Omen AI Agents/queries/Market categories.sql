/*
======= Query Info =======                 
-- query_id: 3582989                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:44.415427                 
-- owner: hdser                 
==========================
*/

WITH


omen_gnosis_trades_liquidity AS (
    SELECT tx_from, fixedproductmarketmaker FROM omen_gnosis.trades
    UNION ALL
    SELECT tx_from, fixedproductmarketmaker FROM omen_gnosis.liquidity
),

omen_gnosis_markets AS (
    SELECT * FROM query_3668567
),

ai_agents_traders AS (
    SELECT * FROM dune.hdser.query_3582994
),

ai_agents_makers AS (
    SELECT * FROM dune.hdser.query_3584116
)


SELECT
    t3.category
    ,COUNT(DISTINCT CASE WHEN t2.address = t1.tx_from THEN t1.fixedproductmarketmaker END) AS cnt_agent_trader
    ,COUNT(DISTINCT CASE WHEN t4.address = t1.tx_from THEN t1.fixedproductmarketmaker END) AS cnt_agent_maker
    ,COUNT(DISTINCT t1.fixedproductmarketmaker) AS cnt
FROM 
    omen_gnosis_trades_liquidity AS t1
LEFT JOIN 
    ai_agents_traders AS t2
    ON t2.address = t1.tx_from
LEFT JOIN 
    ai_agents_makers AS t4
    ON t4.address = t1.tx_from
INNER JOIN
    omen_gnosis_markets AS t3
    ON
    t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
GROUP BY
    1