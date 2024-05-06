-- query_id: 3632482

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

final AS (
    SELECT
        category
        ,volume_agent_trader
        ,volume
        ,SUM(volume_agent_trader) OVER () AS tot_volume_agent_trader
        ,SUM(volume) OVER () AS tot_volume
    FROM (
        SELECT
            t3.category
            ,SUM(CASE WHEN t2.address = t1.tx_from THEN t1.amount/POWER(10,t4.decimals)*t4.price ELSE 0 END) AS volume_agent_trader
            ,SUM(t1.amount/POWER(10,t4.decimals)*t4.price) AS volume
        FROM 
            omen_gnosis_trades AS t1
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
        LEFT JOIN
            prices.usd t4
            ON t4.contract_address = t3.collateraltoken
            AND t4.blockchain = 'gnosis'
            AND t4.minute = DATE_TRUNC('minute',t1.block_time)
        GROUP BY
            1
    )
)

SELECT 
    category
    ,volume_agent_trader AS "Agent Volume"
    ,volume AS "Volume"
FROM final
WHERE 
    volume/tot_volume >= 0.05
    OR
    volume_agent_trader/tot_volume_agent_trader >= 0.05
