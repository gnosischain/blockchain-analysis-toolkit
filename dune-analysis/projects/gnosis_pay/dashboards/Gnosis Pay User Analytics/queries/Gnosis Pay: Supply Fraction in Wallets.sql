/*
======= Query Info =======                     
-- query_id: 3800357                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=token_address, value=0x5Cb9073902F2035222B9749F8fB0c9BFe5527108, type=enum)]                     
-- last update: 2024-07-25 17:22:53.260315                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_erc20_supply_day AS (
    SELECT
        CAST(block_day AS DATE) AS day
        ,amount
    FROM
        query_3800348
    WHERE 
        token_address = CAST({{token_address}} AS varbinary)
        AND
        block_day >= CURRENT_DATE - INTERVAL '1' YEAR
        AND
        block_day < CURRENT_DATE
),

gnosis_gp_wallet_flows_balance AS (
    SELECT 
        t1.block_date
        ,t1.entity_id
        ,SUM(t1.balance_diff) AS balance_diff
    FROM query_3796435 t1 --gnosis_gp_entities_daily_balance_diff
    INNER JOIN 
        query_3707804 t2 --gnosis_gp_users
        ON t2.pay_wallet = t1.user
    WHERE
        t1.token_address = CAST({{token_address}} AS varbinary)
    GROUP BY 
        1, 2
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
        ,day
    FROM
        entity_id_dates
        ,UNNEST(SEQUENCE(CAST(block_date AS DATE), CURRENT_DATE, INTERVAL '1' DAY)) s(day)
),

balance_dense AS (
    SELECT
        day
       ,entity_id
       ,LAST_VALUE(value) IGNORE NULLS OVER (PARTITION BY entity_id ORDER BY day) AS value
    FROM (
    SELECT
        t2.day
        ,t2.entity_id
        ,SUM(t1.balance_diff) OVER (PARTITION BY t2.entity_id ORDER BY t2.day) AS value
    FROM 
        gnosis_gp_wallet_flows_balance t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.block_date
        AND
        t2.entity_id = t1.entity_id
     ) 
)

SELECT 
    t1.day
   ,t1.value/POWER(10,COALESCE(t2.decimals,18)) AS Balance
   ,LAST_VALUE(t3.amount) IGNORE NULLS OVER (ORDER BY t1.day) AS Supply
   ,t1.value/POWER(10,COALESCE(t2.decimals,18))/(LAST_VALUE(t3.amount) IGNORE NULLS OVER (ORDER BY t1.day)) AS "In Wallets"
FROM (
    SELECT 
        day
        ,SUM(value) AS value
    FROM
        balance_dense 
    GROUP BY 1
) t1
LEFT JOIN
    tokens.erc20 t2
    ON 
    t2.contract_address = CAST({{token_address}} AS varbinary)
LEFT JOIN
    gnosis_erc20_supply_day t3
    ON
    t3.day = t1.day
WHERE
    t1.day >= CURRENT_DATE - INTERVAL '1' YEAR
    AND
    t1.day < CURRENT_DATE