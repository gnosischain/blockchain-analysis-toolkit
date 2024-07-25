/*
======= Query Info =======                     
-- query_id: 3869915                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.274298                     
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
    
agg_transfers AS (
    SELECT
        block_date
        ,wallet_address
        ,counterparty
        ,token_address
        ,SUM(amount_raw) AS amount_raw
    FROM (
        SELECT
            t1.block_date
            ,t1."from" AS wallet_address
            ,t1."to" AS counterparty
            ,t1.contract_address AS token_address
            ,-SUM(CAST(t1.amount_raw AS INT256)) AS amount_raw
        FROM 
            tokens_gnosis.transfers t1
        INNER JOIN
            crc_user_info t2
            ON
          --  t2.user = t1."from"
          --  AND
            t2.token = t1.contract_address
        GROUP BY
            1, 2, 3, 4
            
        UNION ALL
        
        SELECT
            t1.block_date
            ,t1."to" AS wallet_address
            ,t1."from" AS counterparty
            ,t1.contract_address AS token_address
            ,SUM(CAST(t1.amount_raw AS INT256)) AS amount_raw
        FROM 
            tokens_gnosis.transfers t1
        INNER JOIN
            crc_user_info t2
            ON
           -- t2.user = t1."to"
          --  AND
            t2.token = t1.contract_address
        GROUP BY
            1, 2, 3, 4
    )
    GROUP BY
            1, 2, 3, 4
)
      

SELECT * FROM agg_transfers