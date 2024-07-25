/*
======= Query Info =======                     
-- query_id: 3821599                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.310466                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_metri_wallet_actions_cnt AS (
    SELECT
        block_date
        ,action
        ,cnts
    FROM 
        query_3821527
    WHERE 
        method = 'Created'
),

calendar AS (
    SELECT 
        block_date
        ,action
    FROM (
        SELECT
            action
            ,MIN(block_date) AS min_block_date
        FROM
            gnosis_metri_wallet_actions_cnt
        GROUP BY 
            1
    ),
    UNNEST(SEQUENCE(min_block_date, CURRENT_DATE - INTERVAL '1' DAY, INTERVAL '1' DAY)) s(block_date)
)

SELECT 
    t1.block_date
    ,t1.action
    ,COALESCE(t2.cnts,0) AS cnts
FROM 
    calendar t1
LEFT JOIN
    gnosis_metri_wallet_actions_cnt t2
    ON 
    t2.block_date = t1.block_date
    AND
    t2.action = t1.action
WHERE
    t1.block_date < CURRENT_DATE

