-- query_id: 3583058

WITH

omen_gnosis_trades AS (
    SELECT * FROM omen_gnosis.trades
),

omen_gnosis_markets AS (
    SELECT * FROM dune.hdser.result_omen_gnosis_markets_mv
),

ai_agents_traders AS (
    SELECT * FROM dune.hdser.query_3582994
),

ai_agents_makers AS (
    SELECT * FROM dune.hdser.query_3584116
),

ai_agents AS (
    SELECT DISTINCT 
        address,
        label
    FROM (
        SELECT * FROM ai_agents_traders
        UNION ALL
        SELECT * FROM ai_agents_makers
    )
)



SELECT
    DATE_TRUNC('day', t1.block_time) AS day,
    t1.action,
    COUNT(*) AS n_trades,
    NULLIF(COUNT(CASE WHEN t2.address = t1.tx_from THEN 1 END),0) AS n_trades_agents,
    SUM(t1.amount/POWER(10,t4.decimals)*t4.price) AS amount_usd,
    NULLIF(SUM(CASE WHEN t2.address = t1.tx_from THEN t1.amount/POWER(10,t4.decimals)*t4.price ELSE 0 END),0) AS amount_agents_usd
FROM 
    omen_gnosis_trades AS t1
LEFT JOIN 
    ai_agents AS t2
    ON t2.address = t1.tx_from
LEFT JOIN
    omen_gnosis_markets AS t3
    ON t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
LEFT JOIN
    prices.usd t4
    ON t4.contract_address = t3.collateraltoken
    AND t4.blockchain = 'gnosis'
    AND t4.minute = DATE_TRUNC('minute',t1.block_time)
--WHERE
--    t1.amount_index = 0  /* count single trade */
GROUP BY
  1,
  2