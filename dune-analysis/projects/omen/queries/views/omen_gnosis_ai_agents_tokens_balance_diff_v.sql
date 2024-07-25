/*
======= Query Info =======                     
-- query_id: 3715148                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.788275                     
-- owner: hdser                     
==========================
*/

WITH

omen_gnosis_ai_agents AS (
    SELECT * FROM query_3582994 --omen_gnosis_ai_agents_traders
    UNION ALL
    SELECT * FROM query_3584116 --omen_gnosis_ai_agents_makers
),

wxdai_deposits AS (
    SELECT 
        t1.evt_block_time
        ,t1.evt_index
        ,t1.dst AS address
        ,t2.label
        ,t1.contract_address AS token_address
        ,CAST(t1.wad AS INT256) AS value
    FROM 
        wxdai_gnosis.WXDAI_evt_Deposit t1
    INNER JOIN
        omen_gnosis_ai_agents t2
        ON 
        t2.address = t1.dst
),

wxdai_withdrawals AS (
    SELECT 
        t1.evt_block_time
        ,t1.evt_index
        ,t1.src AS address
        ,t2.label
        ,t1.contract_address AS token_address
        ,-CAST(t1.wad AS INT256) AS value
    FROM 
        wxdai_gnosis.WXDAI_evt_Withdrawal t1
    INNER JOIN
        omen_gnosis_ai_agents t2
        ON 
        t2.address = t1.src
),

wxdai_transfers AS (
    SELECT 
        t1.evt_block_time
        ,t1.evt_index
        ,t1.src AS address
        ,t2.label
        ,t1.contract_address AS token_address
        ,-CAST(t1.wad AS INT256) AS value
    FROM 
        wxdai_gnosis.WXDAI_evt_Transfer t1
    INNER JOIN
        omen_gnosis_ai_agents t2
        ON 
        t2.address = t1.src
    
    UNION ALL
    
    SELECT 
        t1.evt_block_time
        ,t1.evt_index
        ,t1.dst AS address
        ,t2.label
        ,t1.contract_address AS token_address
        ,CAST(t1.wad AS INT256) AS value
    FROM 
        wxdai_gnosis.WXDAI_evt_Transfer t1
    INNER JOIN
        omen_gnosis_ai_agents t2
        ON 
        t2.address = t1.dst
),

wxdai_balance_diff AS (
    SELECT * FROM wxdai_deposits
    UNION ALL
    SELECT * FROM wxdai_withdrawals
    UNION ALL
    SELECT * FROM wxdai_transfers
),

other_erc20_balance_diff AS (
    SELECT
        t1.evt_block_time
        ,t1.evt_index
        ,t1."from" AS address
        ,t2.label
        ,t1.contract_address AS token_address
        ,-CAST(t1.value AS INT256) AS value
        FROM
            erc20_gnosis.evt_transfer t1
        INNER JOIN
        omen_gnosis_ai_agents t2
        ON 
        t2.address = t1."from"
        WHERE
            t1.contract_address != 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d
        
    UNION ALL
    
    SELECT
        t1.evt_block_time
        ,t1.evt_index
        ,t1.to AS address
        ,t2.label
        ,t1.contract_address AS token_address
        ,CAST(t1.value AS INT256) AS value
        FROM
            erc20_gnosis.evt_transfer t1
        INNER JOIN
        omen_gnosis_ai_agents t2
        ON 
        t2.address = t1.to
        WHERE
            t1.contract_address != 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d
)

SELECT * FROM wxdai_balance_diff
UNION ALL
SELECT * FROM other_erc20_balance_diff