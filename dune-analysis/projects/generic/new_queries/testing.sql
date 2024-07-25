/*
======= Query Info =======                     
-- query_id: 3936000                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:52.415096                     
-- owner: hdser                     
==========================
*/



    SELECT 
     'gnosis' as blockchain
        , t.block_date
        , t.block_time
        , t.block_number
        , t.tx_hash
        , t.evt_index
        , t.trace_address
        , t.token_standard
        , tx."from" AS tx_from
        , tx."to" AS tx_to
        , tx."index" AS tx_index
        , t."from"
        , t.to
        , t.contract_address
        , t.amount_raw
    FROM (
    SELECT
        block_date
        , block_time
        , block_number
        , tx_hash
        , cast(NULL as bigint) AS evt_index
        , trace_address
        , CAST(NULL AS varbinary) AS contract_address 
        , 'native' AS token_standard
        , "from"
        , COALESCE(to,address) AS to -- Contract Creation has NULL "to" address, but transaction might have value that goes to contract created
        , value AS amount_raw
    FROM gnosis.traces
    WHERE success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > UINT256 '0'
        
    UNION ALL
    
     SELECT 
        cast(date_trunc('day', t.evt_block_time) as date) AS block_date
        , t.evt_block_time AS block_time
        , t.evt_block_number AS block_number
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , t.contract_address
        , CASE
            WHEN t.contract_address = CAST(NULL AS varbinary)
            THEN 'native'
            ELSE 'erc20'
            END AS token_standard
        , t."from"
        , t.to
        , t.value AS amount_raw
    FROM erc20_gnosis.evt_transfer t
    ) t
     INNER JOIN "delta_prod"."gnosis"."transactions" tx ON
        cast(date_trunc('day', tx.block_time) as date) = t.block_date 
        AND tx.block_number = t.block_number
        AND tx.hash = t.tx_hash
        
    UNION ALL


    SELECT
         'gnosis' as blockchain
        , cast(date_trunc('day', t.block_time) as date) as block_date
        , t.block_time
        , t.block_number
        , t.tx_hash
        , t.evt_index
        , t.trace_address
        , t.token_standard
        , tx."from" AS tx_from
        , tx."to" AS tx_to
        , tx."index" AS tx_index
        , t."from"
        , t.to
        , t.contract_address
        , t.amount_raw
    FROM (
    SELECT
        t.evt_block_time AS block_time
        , t.evt_block_number AS block_number
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , t.contract_address
        , 'erc20' AS token_standard -- technically this is not a standard 20 token, but we use it for consistency
        , 0x0000000000000000000000000000000000000000 AS "from"
        , t.dst as "to"
        , t.wad AS amount_raw -- is this safe cross chain?
    FROM wxdai_gnosis.WXDAI_evt_Deposit t
    UNION ALL
    SELECT
        t.evt_block_time AS block_time
        , t.evt_block_number AS block_number
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , t.contract_address
        , 'erc20' AS token_standard -- technically this is not a standard 20 token, but we use it for consistency
        , t.src as "from"
        , 0x0000000000000000000000000000000000000000 AS "to"
        , t.wad AS amount_raw -- is this safe cross chain?
    FROM wxdai_gnosis.WXDAI_evt_Withdrawal t
    ) t 
    INNER JOIN "delta_prod"."gnosis"."transactions" tx ON
        tx.block_number = t.block_number
        AND tx.hash = t.tx_hash

