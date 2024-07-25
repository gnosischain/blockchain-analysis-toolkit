/*
======= Query Info =======                     
-- query_id: 3870104                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.372463                     
-- owner: hdser                     
==========================
*/

WITH

tc_balances_diff AS (
    SELECT
     * 
     FROM
      query_3870468 --gnosis_circles_balance_diff_tc_v
),

tc_balances_diff_day AS (
    SELECT
        CAST(block_time AS DATE) AS block_day
        ,SUM(amount_raw) AS amount_raw
        ,SUM(TC_value_raw) AS TC_value_raw
    FROM
        tc_balances_diff
    GROUP BY
        1
),

calendar AS (
    SELECT 
        block_day
    FROM (
        SELECT 
            MIN(block_day) AS block_day_min
        FROM
            tc_balances_diff_day
    )
    ,UNNEST(SEQUENCE(block_day_min,CURRENT_DATE, INTERVAL '1' DAY)) s(block_day)
),

tc_balances_diff_dense AS (
    SELECT 
        t2.block_day
        ,COALESCE(t1.amount_raw,0) AS amount_raw
        ,COALESCE(t1.TC_value_raw,0) AS TC_value_raw
    FROM
        tc_balances_diff_day t1
    RIGHT JOIN
        calendar t2
        ON
        t2.block_day = t1.block_day
),

balances AS (
    SELECT
        block_day
        ,SUM(amount_raw) OVER (ORDER BY block_day) AS amount_raw
        ,SUM(TC_value_raw) OVER (ORDER BY block_day) AS TC_value_raw
    FROM 
        tc_balances_diff_dense 
)

SELECT 
    block_day
    ,amount_raw/POWER(10,18) AS "CRC"
    ,TC_value_raw/POWER(10,18) AS "Time CRC"
FROM balances

