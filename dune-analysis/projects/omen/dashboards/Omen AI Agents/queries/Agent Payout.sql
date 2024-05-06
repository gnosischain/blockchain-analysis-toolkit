-- query_id: 3584660

WITH

omen_gnosis_payouts AS (
    SELECT * FROM omen_gnosis.ConditionalTokens_evt_PayoutRedemption
),


ai_agents_traders AS (
    SELECT * FROM dune.hdser.query_3582994
)

SELECT
    day
    ,payout_usd
    ,SUM(payout_usd) OVER (ORDER BY day) AS cumsum_payout_usd
FROM (
    SELECT 
        DATE_TRUNC('day',evt_block_time) AS day
        ,SUM(payout/POWER(10,t4.decimals)*t4.price) AS payout_usd
    FROM
        omen_gnosis_payouts t1
    INNER JOIN
        ai_agents_traders AS t2
        ON t2.address = t1.evt_tx_from OR t2.address = t1.evt_tx_to
    LEFT JOIN
            prices.usd t4
            ON t4.contract_address = t1.collateralToken
            AND t4.blockchain = 'gnosis'
            AND t4.minute = DATE_TRUNC('minute',t1.evt_block_time)
    WHERE
        t2.label = '{{agent_traders}}'
    GROUP BY 
        1
)
ORDER BY
    1 DESC