/*
======= Query Info =======                     
-- query_id: 3650439                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.079975                     
-- owner: hdser                     
==========================
*/

WITH 

deposit AS (
    SELECT 
        evt_block_time
        ,evt_block_date
        ,"from" AS user
        ,CAST(value AS INT256) AS value
    FROM erc20_gnosis.evt_transfer
    WHERE
        contract_address = 0xd4Ca39f78Bf14BfaB75226AC833b1858dB16f9a1
        AND
        to = 0xd4Ca39f78Bf14BfaB75226AC833b1858dB16f9a1
        AND
        evt_block_date >= DATE '2022-01-01'
),

withdrawal AS (
    SELECT 
        evt_block_time
        ,evt_block_date
        ,to AS user
        ,CAST(-value AS INT256) AS value
    FROM erc20_gnosis.evt_transfer
    WHERE
        contract_address = 0xd4Ca39f78Bf14BfaB75226AC833b1858dB16f9a1
        AND
        "from" = 0xd4Ca39f78Bf14BfaB75226AC833b1858dB16f9a1
        AND
        evt_block_date >= DATE '2022-01-01'
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