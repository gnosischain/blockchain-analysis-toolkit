/*
======= Query Info =======                     
-- query_id: 3800659                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=address, value=default value, type=text), Parameter(name=from_date, value=2024-01-01 00:00:00, type=datetime), Parameter(name=to_date, value=2024-06-07 00:00:00, type=datetime), Parameter(name=token_address, value=default value, type=text)]                     
-- last update: 2024-07-25 17:22:48.854474                     
-- owner: hdser                     
==========================
*/

WITH

wallet_balance_diff AS (
    SELECT 
        block_day AS block_date
        ,SUM(amount_raw) AS balance_diff
    FROM test_schema.git_dunesql_0f2979a_transfers_gnosis_erc20_agg_day
    WHERE
        token_address = CAST({{token_address}} AS varbinary)
        AND
        wallet_address = CAST({{address}} AS varbinary)
    GROUP BY 
        1
),

dates AS (
    SELECT 
        MIN(block_date) AS block_date
    FROM wallet_balance_diff
),

calendar AS (
    SELECT 
        day
    FROM
        dates
        ,UNNEST(SEQUENCE(CAST(block_date AS DATE), CURRENT_DATE, INTERVAL '1' DAY)) s(day)
),

balance_dense AS (
    SELECT
        day
        ,balance_diff
       ,LAST_VALUE(value) IGNORE NULLS OVER (ORDER BY day) AS value
    FROM (
        SELECT
           t2.day
           ,COALESCE(t1.balance_diff, 0) AS balance_diff
           ,SUM(t1.balance_diff) OVER (ORDER BY t2.day) AS value
        FROM 
            wallet_balance_diff t1
        RIGHT JOIN
            calendar t2
            ON
            t2.day = t1.block_date
     ) 
)

SELECT 
    day
    ,balance_diff/POWER(10,COALESCE(decimals,18)) AS balance_diff
    ,value/POWER(10,COALESCE(decimals,18)) AS value
    ,AVG(value/POWER(10,COALESCE(decimals,18))) OVER () AS avg_value
FROM 
    balance_dense t1
LEFT JOIN
    tokens.erc20 t2
    ON 
    t2.contract_address = CAST({{token_address}} AS varbinary)
WHERE
    day >=  CAST('{{from_date}}' AS TIMESTAMP)
    AND
    day <=  CAST('{{to_date}}' AS TIMESTAMP)