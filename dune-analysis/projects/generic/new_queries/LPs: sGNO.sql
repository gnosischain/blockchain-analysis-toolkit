/*
======= Query Info =======                     
-- query_id: 3661283                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:57.103825                     
-- owner: hdser                     
==========================
*/

WITH 

inflow AS (
    SELECT 
        evt_block_time
        ,evt_block_date
        ,to AS user
        ,CAST(value AS INT256) AS value
    FROM erc20_gnosis.evt_transfer
    WHERE
        contract_address in (0xbdf4488dcf7165788d438b62b4c8a333879b7078,0x2686d5E477d1AaA58BF8cE598fA95d97985c7Fb1)
        AND
        evt_block_date >= DATE '2022-01-01'
),

outflow AS (
    SELECT 
        evt_block_time
        ,evt_block_date
        ,"from" AS user
        ,CAST(-value AS INT256) AS value
    FROM erc20_gnosis.evt_transfer
    WHERE
        contract_address in (0xbdf4488dcf7165788d438b62b4c8a333879b7078,0x2686d5E477d1AaA58BF8cE598fA95d97985c7Fb1)
        AND
        evt_block_date >= DATE '2022-01-01'
),

balance_diff AS (
    SELECT
        evt_block_date
        ,user
        ,SUM(value) AS value
    FROM (
        SELECT * FROM inflow
        UNION ALL
        SELECT * FROM outflow
    )
    WHERE
        user != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),

distinct_users AS (
    SELECT DISTINCT user FROM balance_diff
),

calendar AS (
    SELECT date FROM UNNEST(sequence(DATE '2022-02-14', DATE '2023-02-15')) t(date)
),

users_daily AS (
    SELECT 
        t1.user
        ,t2.date
    FROM
        distinct_users t1
    CROSS JOIN 
        calendar t2
),

final AS (
    SELECT 
        date
        ,user
        ,(SUM(value) OVER (PARTITION BY user ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))/1e18 AS balance
    FROM (
        SELECT 
            t2.date
            ,t2.user
            ,COALESCE(t1.value,0)  AS value
        FROM 
            balance_diff t1
        RIGHT JOIN
            users_daily t2
            ON 
            t2.date = t1.evt_block_date
            AND
            t2.user = t1.user
    )
)

SELECT * FROM final
WHERE balance != 0
ORDER BY 1, 2
