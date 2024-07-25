/*
======= Query Info =======                     
-- query_id: 3713406                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.592894                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_gp_wallet_flows_balance AS (
    SELECT * FROM query_3713262
    WHERE token_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E
),

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

calendar AS (
    SELECT 
        entity_id
        ,action
        ,day
    FROM
        gnosis_gp_users
        ,UNNEST(SEQUENCE(CAST(creation_time AS DATE), CURRENT_DATE, INTERVAL '1' DAY)) s(day)
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
    t2.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E
