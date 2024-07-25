/*
======= Query Info =======                     
-- query_id: 3897572                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=from_date, value=2024-06-01 00:00:00, type=datetime), Parameter(name=to_date, value=2024-07-04 00:00:00, type=datetime)]                     
-- last update: 2024-07-25 17:22:51.674619                     
-- owner: hdser                     
==========================
*/

WITH

crc_new_users AS (
    SELECT 
        evt_block_date AS block_date
        ,COUNT(*) AS cnt
    FROM circles_ubi_gnosis.Hub_evt_Signup
    WHERE
        evt_block_date >=  CAST('{{from_date}}' AS TIMESTAMP)
        AND
        evt_block_date <=  CAST('{{to_date}}' AS TIMESTAMP)
    GROUP BY
        1
),

Hub_evt_Trust AS (
    SELECT
        evt_block_date AS block_date
        ,COUNT(*) AS trust_cnt
    FROM circles_ubi_gnosis.Hub_evt_Trust 
     WHERE
        evt_block_date >=  CAST('{{from_date}}' AS TIMESTAMP)
        AND
        evt_block_date <=  CAST('{{to_date}}' AS TIMESTAMP)
    GROUP BY
        1
)

SELECT 
    t1.block_date
    ,t1.cnt AS "New Accounts"
    ,COALESCE(t2.trust_cnt,0) AS "Trusts"
FROM 
    crc_new_users t1
LEFT JOIN
    Hub_evt_Trust t2
    ON
    t2.block_date = t1.block_date