-- query_id: 3584516

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

trades_list AS (
    SELECT DISTINCT
        t1.block_time
        ,action
        ,t1.outcomeIndex AS outcome
        ,t1.amount
        ,t1.amount/POWER(10,t4.decimals)*t4.price AS amount_usd
        ,t3.question
        ,t3.fixedproductmarketmaker
    FROM
        omen_gnosis_trades t1
    INNER JOIN
        ai_agents_traders AS t2
        ON t2.address = t1.tx_from OR t2.address = t1.tx_to
    INNER JOIN
        omen_gnosis_markets t3
        ON t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        prices.usd t4
        ON t4.contract_address = t3.collateraltoken
        AND t4.blockchain = 'gnosis'
        AND t4.minute = DATE_TRUNC('minute',t1.block_time)
    WHERE
        t1.action = 'Buy'
        AND
        t2.label = '{{agent_traders}}'
),

daily_volume AS (
    SELECT 
        *
        ,SUM(amount_usd) OVER (ORDER BY day) AS cumsum_amount_usd
        ,SUM(cnt) OVER (ORDER BY day) AS cumsum_cnt
    FROM (
        SELECT
            DATE_TRUNC('day',block_time) AS day
            ,SUM(amount_usd) AS amount_usd
            ,COUNT(*) AS cnt
        FROM
            trades_list
        GROUP BY 1
    )
)

SELECT  * FROM daily_volume
ORDER BY 1 DESC