/*
======= Query Info =======                 
-- query_id: 3796476                 
-- description: ""                 
-- tags: []                 
-- parameters: [Parameter(name=token_address, value=0x5Cb9073902F2035222B9749F8fB0c9BFe5527108, type=enum)]                 
-- last update: 2024-07-25 17:22:45.820040                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gp_wallet_flows_balance AS (
    SELECT 
        t1.block_date
        ,t1.entity_id
        ,t1.user
        ,t1.action
        ,SUM(t1.balance_diff) AS balance_diff
    FROM query_3796435 t1 --gnosis_gp_entities_daily_balance_diff
    INNER JOIN 
        query_3707804 t2 --gnosis_gp_users
        ON t2.pay_wallet = t1.user
    WHERE
        t1.token_address = CAST({{token_address}} AS varbinary)
    GROUP BY 
        1, 2, 3, 4
),

entity_id_dates AS (
    SELECT 
        entity_id
        ,MIN(block_date) AS block_date
    FROM gnosis_gp_wallet_flows_balance
    GROUP BY 1
),

calendar AS (
    SELECT 
        entity_id
        ,action
        ,day
    FROM
        entity_id_dates
        ,UNNEST(SEQUENCE(CAST(block_date AS DATE), CURRENT_DATE, INTERVAL '1' DAY)) s(day)
        ,UNNEST(ARRAY['Inflow','Outflow','Mint','Burn','Balance']) t(action)
),

balance_dense AS (
    SELECT
        day
       ,entity_id
       ,action
       ,LAST_VALUE(value) IGNORE NULLS OVER (PARTITION BY entity_id, action ORDER BY day) AS value
    FROM (
    SELECT
       t2.day
       ,t2.entity_id
       ,t2.action
       ,IF(t2.action = 'Balance',
            SUM(t1.balance_diff) OVER (PARTITION BY t2.entity_id ORDER BY t2.day),
            COALESCE(t1.balance_diff,0)
        ) AS value
    FROM 
        gnosis_gp_wallet_flows_balance t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.block_date
        AND
        t2.entity_id = t1.entity_id
        AND
        t2.action = t1.action
     ) 
)

SELECT 
    day
   ,action
   ,value/POWER(10,COALESCE(decimals,18)) AS value
   ,LAST_VALUE(current_balance/POWER(10,COALESCE(decimals,18))) IGNORE NULLS OVER () AS current_balance
   ,total_Outflow/POWER(10,COALESCE(decimals,18)) AS total_Outflow
   ,total_Inflow/POWER(10,COALESCE(decimals,18)) AS total_Inflow
   ,total_Burn/POWER(10,COALESCE(decimals,18)) AS total_Burn
   ,total_Mint/POWER(10,COALESCE(decimals,18)) AS total_Mint
FROM (
    SELECT
        t1.day
       ,t1.action
       ,t1.value AS value
       ,IF(t1.action = 'Balance' AND t1.day = CURRENT_DATE - INTERVAL '1' DAY, t1.value, NULL) AS current_balance
       ,SUM(IF(t1.action = 'Outflow',t1.value, 0)) OVER () AS total_Outflow
       ,SUM(IF(t1.action = 'Inflow',t1.value, 0)) OVER () AS total_Inflow
       ,SUM(IF(t1.action = 'Burn',t1.value, 0)) OVER () AS total_Burn
       ,SUM(IF(t1.action = 'Mint',t1.value, 0)) OVER () AS total_Mint
       ,t2.decimals
    FROM (
        SELECT 
            day
           ,action
           ,SUM(value) AS value
        FROM
            balance_dense 
        GROUP BY 1,2
    ) t1
    LEFT JOIN
        tokens.erc20 t2
        ON 
        t2.contract_address = CAST({{token_address}} AS varbinary)
)
WHERE
    day >= CURRENT_DATE - INTERVAL '1' YEAR
    AND
    day < CURRENT_DATE