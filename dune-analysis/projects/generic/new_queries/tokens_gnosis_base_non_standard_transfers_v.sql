/*
======= Query Info =======                     
-- query_id: 3935671                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.903506                     
-- owner: hdser                     
==========================
*/



WITH 

gas_fee as (
    SELECT 
        'gas_fee' as transfer_type
        ,cast(date_trunc('day', block_time) as date) AS block_date
        , block_time AS block_time
        , block_number AS block_number
        , hash AS tx_hash
        , NULL AS evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , "from"
        , CAST(NULL AS varbinary) AS to
        , gas_used * gas_price AS amount_raw
    FROM 
        gnosis.transactions
    WHERE   
        success
    
),

gas_fee_collection as (
    SELECT 
        'gas_fee_collection' as transfer_type
        ,cast(date_trunc('day', t1.block_time) as date) AS block_date
        , t1.block_time AS block_time
        , t1.block_number AS block_number
        , t1.hash AS tx_hash
        , NULL AS evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , 0x6BBe78ee9e474842Dbd4AB4987b3CeFE88426A92 AS to -- fee collector
        , t1.gas_used * COALESCE(t2.base_fee_per_gas, CAST(0 AS UINT256)) AS amount_raw
    FROM 
        gnosis.transactions t1
    INNER JOIN
        gnosis.blocks t2
        ON
        t2.number = t1.block_number
    WHERE   
        t1.success
    
),

gas_fee_rewards as (
    SELECT 
        'gas_fee_rewards' as transfer_type
        ,cast(date_trunc('day', t1.block_time) as date) AS block_date
        , t1.block_time AS block_time
        , t1.block_number AS block_number
        , t1.hash AS tx_hash
        , NULL AS evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , t2.miner AS to 
        , IF(t1.gas_price = CAST(0 AS UINT256),
            CAST(0 AS UINT256),
            t1.gas_used * ( t1.gas_price - COALESCE(t2.base_fee_per_gas,CAST(0 AS UINT256)) )
        ) AS amount_raw
    FROM 
        gnosis.transactions t1
    INNER JOIN
        gnosis.blocks t2
        ON
        t2.number = t1.block_number
    WHERE   
        t1.success
     
),

block_reward AS (
    SELECT 
        'block_reward' as transfer_type
        ,cast(date_trunc('day', evt_block_time) as date) AS block_date
        , evt_block_time AS block_time
        , evt_block_number AS block_number
        , evt_tx_hash AS tx_hash
        , evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , receiver AS to 
        , amount AS amount_raw
    FROM 
        xdai_gnosis.RewardByBlock_evt_AddedReceiver
    

    UNION ALL

    SELECT 
        'block_reward' as transfer_type
        , cast(date_trunc('day', evt_block_time) as date) AS block_date
        , evt_block_time AS block_time
        , evt_block_number AS block_number
        , evt_tx_hash AS tx_hash
        , evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary)  AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , receiver AS to 
        , amount AS amount_raw
    FROM 
        xdai_gnosis.BlockRewardAuRa_evt_AddedReceiver
    
),

non_standard_transfers AS (
    SELECT * FROM gas_fee
    UNION ALL
    SELECT * FROM gas_fee_collection
    UNION ALL
    SELECT * FROM gas_fee_rewards
    UNION ALL
    SELECT * FROM block_reward
)


SELECT 
     t.transfer_type
    , 'gnosis' as blockchain
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
FROM non_standard_transfers t
INNER JOIN gnosis.transactions tx ON
    cast(date_trunc('day', tx.block_time) as date) = t.block_date 
    AND tx.block_number = t.block_number
    AND tx.hash = t.tx_hash
WHERE  
    t.amount_raw > UINT256 '0'