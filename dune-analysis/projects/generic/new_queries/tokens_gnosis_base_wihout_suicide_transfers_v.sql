/*
======= Query Info =======                     
-- query_id: 3935675                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.098438                     
-- owner: hdser                     
==========================
*/



WITH 

tokens_gnosis_base_transfers AS (
    SELECT 
         blockchain
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address
        , amount_raw
    FROM 
        query_3935643 --tokens_gnosis_base_transfers_v
    
),

tokens_gnosis_base_non_standard_transfers AS (
    SELECT 
         blockchain
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address
        , amount_raw
    FROM 
        query_3935671 --tokens_gnosis_base_non_standard_transfers_v
    
)

SELECT * FROM tokens_gnosis_base_transfers
UNION ALL 
SELECT * FROM tokens_gnosis_base_non_standard_transfers