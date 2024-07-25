/*
======= Query Info =======                     
-- query_id: 3870408                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.925793                     
-- owner: hdser                     
==========================
*/


WITH

users_mints AS (
    SELECT DISTINCT
        block_date
        ,wallet_address
    FROM 
        query_3869915 --gnosis_circles_transfers_agg_day_v
    WHERE
        counterparty = 0x0000000000000000000000000000000000000000
),

daily_users AS (
    SELECT
        block_date
        ,COUNT(*) AS cnt
    FROM
        users_mints
    GROUP BY 1
),

weekly_users AS (
    SELECT
        DATE_TRUNC('week', block_date) AS block_date
        ,COUNT(DISTINCT wallet_address) AS cnt
    FROM
        users_mints
    GROUP BY 1
),

monthly_users AS (
    SELECT
        DATE_TRUNC('month', block_date) AS block_date
        ,COUNT(DISTINCT wallet_address) AS cnt
    FROM
        users_mints
    GROUP BY 1
)

SELECT
    block_date
    ,daily_users_mint AS daily
    ,LAST_VALUE(weekly_users_mint) IGNORE NULLS OVER (ORDER BY block_date) AS weekly
    ,LAST_VALUE(monthly_users_mint) IGNORE NULLS OVER (ORDER BY block_date) AS monthly
FROM (
    SELECT 
        t1.block_date
        ,t1.cnt AS daily_users_mint
        ,t2.cnt AS weekly_users_mint
        ,t3.cnt AS monthly_users_mint
    FROM 
        daily_users t1
    LEFT JOIN 
        weekly_users t2
        ON
        t2.block_date = t1.block_date
    LEFT JOIN 
        monthly_users t3
         ON
        t3.block_date = t1.block_date
)