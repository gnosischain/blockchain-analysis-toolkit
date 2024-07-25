/*
======= Query Info =======                     
-- query_id: 3870468                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.606380                     
-- owner: hdser                     
==========================
*/

WITH

balances_diff AS (
        SELECT
            block_time
            ,wallet_address
            ,amount_raw
        FROM
            query_3870087 --gnosis_circles_balance_diff_v
        WHERE
            wallet_address != 0x0000000000000000000000000000000000000000
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
    wallet_address,
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
    wallet_address,
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
  wallet_address,
  amount_raw,
  amount_raw / (previousYearPayout * (1 - daysInCurrentCirclesYear / oneCirclesYearInDays) + currentYearPayout * (daysInCurrentCirclesYear / oneCirclesYearInDays)) * 24 as TC_value_raw
FROM
  payouts,
  parameters
)

SELECT * FROM tc_balances_diff
