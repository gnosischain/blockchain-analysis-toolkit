/*
======= Query Info =======                 
-- query_id: 3713653                 
-- description: ""                 
-- tags: []                 
-- parameters: [Parameter(name=token_address, value=0x5Cb9073902F2035222B9749F8fB0c9BFe5527108, type=enum)]                 
-- last update: 2024-07-25 17:22:45.753823                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gp_wallet_flows_balance AS (
    SELECT * FROM query_3713262
    WHERE token_address = CAST({{token_address}} AS varbinary)
),

entity_id_dates AS (
    SELECT 
        entity_id
        ,MIN(evt_block_date) AS block_date
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
       ,IF(t2.action = 'Balance',t1.value,COALESCE(t1.value,0)) AS value
    FROM 
        gnosis_gp_wallet_flows_balance t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.evt_block_date
        AND
        t2.entity_id = t1.entity_id
        AND
        t2.action = t1.action
     ) 
)

SELECT
    t1.day
   ,t1.action
   ,t1.value/POWER(10,COALESCE(t2.decimals,18)) AS value
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