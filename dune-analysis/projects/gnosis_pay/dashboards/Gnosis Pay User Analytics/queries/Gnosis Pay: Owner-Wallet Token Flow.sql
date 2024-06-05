-- query_id: 3731800

WITH

gnosis_gp_wallet_owner_flows AS (
    SELECT * FROM query_3731893
    WHERE token_address = CAST({{token_address}} AS varbinary)
),

entity_id_dates AS (
    SELECT 
        entity_id
        ,MIN(evt_block_date) AS block_date
    FROM gnosis_gp_wallet_owner_flows
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
        ,UNNEST(ARRAY['Inflow','Outflow']) t(action)
),

balance_dense AS (
    SELECT
       t2.day
       ,t2.entity_id
       ,t2.action
       ,COALESCE(t1.value,0) AS value
    FROM 
        gnosis_gp_wallet_owner_flows t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.evt_block_date
        AND
        t2.entity_id = t1.entity_id
        AND
        t2.action = t1.action
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