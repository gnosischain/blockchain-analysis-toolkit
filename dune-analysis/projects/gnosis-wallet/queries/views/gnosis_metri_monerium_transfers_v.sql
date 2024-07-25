/*
======= Query Info =======                 
-- query_id: 3792681                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:46.808065                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_metri_wallets AS (
    SELECT
         safe_wallet
        ,method
        ,created_at
        ,MIN(imported_at) AS imported_at
    FROM query_3674206
    WHERE 
        (created_at >= DATE '2024-05-01' AND method = 'Created')
        OR
        (imported_at >= DATE '2024-05-01' AND method = 'Imported')
    GROUP BY
    1,2,3
),

metri_monerium_transfers_evts AS (
    SELECT 
        t1.evt_tx_hash
        ,t1.evt_index
        ,t2.method
    FROM
        gnosis_safe_gnosis.GnosisSafeL2_v1_3_0_evt_SafeMultiSigTransaction t1
    INNER JOIN
        gnosis_metri_wallets t2
        ON
        t2.safe_wallet = t1.contract_address
    INNER JOIN --BlacklistValidator_Decision
        gnosis.logs t3
        ON
        t3.tx_hash = t1.evt_tx_hash
        AND
        t3.index = t1.evt_index + 1
    INNER JOIN
         gnosis_safe_gnosis.GnosisSafeL2_v1_3_0_evt_ExecutionSuccess t6
         ON
         t6.evt_tx_hash = t1.evt_tx_hash
         AND
         t6.evt_index = t1.evt_index + 3
    WHERE
        t3.contract_address = 0x3fe4dB892b4572A24B4431ef5742A88EF68FF541
        AND
        t3.topic0 = 0x7421973e31248bda00bf2f04b80b46b34fc9e23ab57848234aed6cc5d437b6a8
        AND
        t3.block_time >= DATE '2024-05-01' 
        AND
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
)



SELECT
    t1.evt_tx_hash
    ,t1.evt_index
    ,t1.evt_block_time
    ,t1.contract_address AS token_address
    ,t1."from" AS wallet_address
    ,t1.to AS counterparty
    ,t1.value AS amount_raw
    ,t2.method
FROM
    monerium_eure_gnosis.EURe_evt_Transfer t1
INNER JOIN
    metri_monerium_transfers_evts t2
    ON
    t2.evt_tx_hash = t1.evt_tx_hash
    AND
    t2.evt_index = t1.evt_index - 2
    
UNION ALL


SELECT
    t1.evt_tx_hash
    ,t1.evt_index
    ,t1.evt_block_time
    ,t1.contract_address AS token_address
    ,t1."from" AS wallet_address
    ,t1.to AS counterparty
    ,t1.value AS amount_raw
    ,t2.method
FROM
    monerium_gbpe_gnosis.GBP_evt_Transfer t1
INNER JOIN
    metri_monerium_transfers_evts t2
    ON
    t2.evt_tx_hash = t1.evt_tx_hash
    AND
    t2.evt_index = t1.evt_index - 2