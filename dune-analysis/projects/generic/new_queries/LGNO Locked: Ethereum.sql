/*
======= Query Info =======                     
-- query_id: 3650607                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.518543                     
-- owner: hdser                     
==========================
*/

WITH 

deposit AS (
    SELECT 
        evt_block_time
        ,DATE_TRUNC('day',evt_block_time) AS evt_block_date
        ,"from" AS user
        ,CAST(value AS INT256) AS value
    FROM erc20_ethereum.evt_transfer
    WHERE
        contract_address = 0x4f8AD938eBA0CD19155a835f617317a6E788c868
        AND
        to = 0x4f8AD938eBA0CD19155a835f617317a6E788c868
        AND
        DATE_TRUNC('day',evt_block_time) >= DATE '2022-01-01'
),

withdrawal AS (
    SELECT 
        evt_block_time
        ,DATE_TRUNC('day',evt_block_time) AS evt_block_date
        ,to AS user
        ,CAST(-value AS INT256) AS value
    FROM erc20_ethereum.evt_transfer
    WHERE
        contract_address = 0x4f8AD938eBA0CD19155a835f617317a6E788c868
        AND
        "from" = 0x4f8AD938eBA0CD19155a835f617317a6E788c868
        AND
        DATE_TRUNC('day',evt_block_time) >= DATE '2022-01-01'
),

balance AS (
    SELECT * FROM deposit
    UNION ALL
    SELECT * FROM withdrawal
)

SELECT 
     user 
    ,value
FROM (
SELECT 
    user
    ,SUM(value)/1e18 AS value
FROM 
    balance
WHERE 
    evt_block_date <= DATE '2022-02-15'
GROUP BY 1
)
WHERE value != 0
ORDER BY 2 DESC