/*
======= Query Info =======                     
-- query_id: 3782625                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=wallet, value=0x800a369d159b8d9605482a56a38ac561dc89aadd, type=enum)]                     
-- last update: 2024-07-25 17:22:56.907801                     
-- owner: hdser                     
==========================
*/

WITH

circle_metadata AS (
    SELECT 
        token AS token_address
        ,'CRC' AS symbol
        ,18 AS decimals
    FROM circles_ubi_gnosis.Hub_evt_Signup
    WHERE
        user =  CAST({{wallet}} AS varbinary)--0xA6247834B41771022498F63CAE8820fFEE208265 --0xA1caF31172a0B5b5C3863ee78a797E1b14b1c39a
),

balances_diff AS (
        SELECT
            t1.evt_block_time AS block_time
            ,SUM(t1.amount_raw) AS amount_raw
        FROM
            test_schema.git_dunesql_b22c009_transfers_gnosis_erc20 t1
        INNER JOIN
            circle_metadata t3
            ON
            t3.token_address = t1.token_address
        WHERE
            t1.wallet_address =  CAST({{wallet}} AS varbinary)
        GROUP BY 1

),

parameters AS (
  SELECT
    TIMESTAMP '2020-10-15 00:00:00 UTC' as circlesInceptionDate,
    365.25 as oneCirclesYearInDays,
    86400 * 1000 as oneDayInMilliSeconds,
    365.25 * 24 * 60 * 60 * 1000 AS oneCirclesYearInMilliSeconds,
    8 as initialDailyCrcPayout,
    1.07 as yearlyInflationRate
), 

time_calculations AS (
  SELECT
    block_time,
    amount_raw,
   (to_unixtime(block_time) - to_unixtime(circlesInceptionDate)) * 1000 as millisecondsSinceInception,
    ((to_unixtime(block_time) - to_unixtime(circlesInceptionDate)) * 1000 / oneDayInMilliSeconds) as daysSinceCirclesInception,
    ((to_unixtime(block_time) - to_unixtime(circlesInceptionDate)) * 1000 / oneCirclesYearInMilliSeconds) as circlesYearsSince,
    MOD(CAST((to_unixtime(block_time) - to_unixtime(circlesInceptionDate)) * 1000 / oneDayInMilliSeconds AS DOUBLE), oneCirclesYearInDays) as daysInCurrentCirclesYear
  FROM
    balances_diff,
    parameters
),

payouts AS (
  SELECT
    block_time,
    amount_raw,
    initialDailyCrcPayout * POWER(yearlyInflationRate, FLOOR(circlesYearsSince)+1) as currentYearPayout,
    initialDailyCrcPayout * POWER(yearlyInflationRate, FLOOR(circlesYearsSince)) as previousYearPayout,
    daysInCurrentCirclesYear
  FROM
    time_calculations,
    parameters
),

tc_balances_diff AS (
SELECT
  block_time,
  amount_raw,
  amount_raw / (previousYearPayout * (1 - daysInCurrentCirclesYear / oneCirclesYearInDays) + currentYearPayout * (daysInCurrentCirclesYear / oneCirclesYearInDays)) * 24 as TC_value_raw
FROM
  payouts,
  parameters
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
ORDER BY 
    block_day DESC

