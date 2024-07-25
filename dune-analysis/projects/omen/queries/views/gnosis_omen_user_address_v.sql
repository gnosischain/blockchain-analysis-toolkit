/*
======= Query Info =======                     
-- query_id: 3667730                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.934489                     
-- owner: hdser                     
==========================
*/

WITH 

ConditionalTokens_evt_TransferBatch AS (
    SELECT 
        *
    FROM
        omen_gnosis.ConditionalTokens_evt_TransferBatch
),

user_id_batch AS (
    --Assumes max 2  TransferBatch with such constraint
    SELECT
        evt_tx_hash
        ,from_list[1] AS fixedproductmarketmaker
        ,CASE
            WHEN from_list[2] = to_list[1] THEN IF(to_list[2]!= 0x0000000000000000000000000000000000000000,to_list[2],to_list[1])
        END AS user
    FROM (
        SELECT
            evt_tx_hash
            ,ARRAY_AGG("from" ORDER BY evt_index) AS from_list
            ,ARRAY_AGG(to ORDER BY evt_index) AS to_list
        FROM 
            ConditionalTokens_evt_TransferBatch
        WHERE
            operator = "from" 
        GROUP BY 1
    )
    WHERE CARDINALITY(from_list) = 2
)

SELECT * FROM user_id_batch