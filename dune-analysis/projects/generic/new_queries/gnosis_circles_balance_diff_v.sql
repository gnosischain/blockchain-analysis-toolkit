/*
======= Query Info =======                     
-- query_id: 3870087                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.746058                     
-- owner: hdser                     
==========================
*/

WITH

crc_user_info AS (
    SELECT 
        user
        ,token
    FROM circles_ubi_gnosis.Hub_evt_Signup
),
    
balance_diff AS (
    SELECT
        block_time
        ,wallet_address
        ,token_address
        ,SUM(amount_raw) AS amount_raw
    FROM (
        SELECT
            t1.block_time
            ,t1."from" AS wallet_address
            ,t1.contract_address As token_address
            ,-SUM(CAST(t1.amount_raw AS INT256)) AS amount_raw
        FROM 
            tokens_gnosis.transfers t1
        INNER JOIN
            crc_user_info t2
            ON
            --t2.user = t1."from"
            --AND
            t2.token = t1.contract_address
        GROUP BY
            1, 2, 3
            
        UNION ALL
        
        SELECT
            t1.block_time
            ,t1."to" AS wallet_address
            ,t1.contract_address As token_address
            ,SUM(CAST(t1.amount_raw AS INT256)) AS amount_raw
        FROM 
            tokens_gnosis.transfers t1
        INNER JOIN
            crc_user_info t2
            ON
           -- t2.user = t1."to"
           -- AND
            t2.token = t1.contract_address
        GROUP BY
            1, 2, 3
    )
    GROUP BY
            1, 2, 3
)
      

SELECT * FROM balance_diff