/*
======= Query Info =======                 
-- query_id: 3797151                 
-- description: ""                 
-- tags: []                 
-- parameters: [Parameter(name=token_address, value=0x5Cb9073902F2035222B9749F8fB0c9BFe5527108, type=enum)]                 
-- last update: 2024-07-25 17:22:46.525081                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gp_wallet_counterparties_inflow AS (
    SELECT
        *
        ,ROW_NUMBER() OVER () AS index
    FROM (
        SELECT 
            IF(t1.counterparty = t2.owner, 
                'Owner',
                CAST(t1.counterparty AS VARCHAR)
            ) AS counterparty 
            ,SUM(t1.balance_diff) AS value
        FROM 
            query_3796435 t1 --
        INNER JOIN 
            query_3707804 t2 --gnosis_gp_users
            ON t2.pay_wallet = t1.user
        WHERE
            t1.token_address = CAST({{token_address}} AS varbinary)
            AND
            t1.action = 'Inflow'
            AND
            t1.block_date < CURRENT_DATE
        GROUP BY
            1
        ORDER BY
            2 DESC
        LIMIT 11
    )
),

gnosis_gp_wallet_counterparties_outflow AS (
    SELECT
        *
        ,ROW_NUMBER() OVER () AS index
    FROM (
        SELECT 
            IF(t1.counterparty = t2.owner, 
                'Owner',
                CAST(t1.counterparty AS VARCHAR)
            ) AS counterparty 
            ,SUM(ABS(t1.balance_diff)) AS value
        FROM 
            query_3796435 t1 --gnosis_gp_entities_daily_balance_diff
        INNER JOIN 
            query_3707804 t2 --gnosis_gp_users
            ON t2.pay_wallet = t1.user
        WHERE
            t1.token_address = CAST({{token_address}} AS varbinary)
            AND
            t1.action = 'Outflow'
        GROUP BY
            1
        ORDER BY
            2 DESC
        LIMIT 11
    )
)

SELECT DISTINCT
    t1.counterparty AS counterparty_inflow
    ,t1.value/POWER(10,18) AS value_infow
    ,t2.counterparty AS counterparty_outflow
    ,t2.value/POWER(10,18) AS value_outfow
FROM 
    gnosis_gp_wallet_counterparties_inflow t1
INNER JOIN
    gnosis_gp_wallet_counterparties_outflow t2
    ON
    t1.index = t2.index
