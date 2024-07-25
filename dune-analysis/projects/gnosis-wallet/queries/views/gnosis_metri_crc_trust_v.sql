/*
======= Query Info =======                     
-- query_id: 3822102                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.177393                     
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

   

gnosis_metri_trust AS (
    SELECT 
         t1.evt_tx_hash
        ,t4.evt_index
        ,t1.evt_block_time
        ,t2.safe_wallet AS wallet_address 
        ,t2.method
    FROM
        gnosis_safe_gnosis.GnosisSafeL2_v1_3_0_evt_SafeMultiSigTransaction t1
    INNER JOIN
        gnosis_metri_wallets t2
        ON
        t2.safe_wallet = t1.contract_address
    INNER JOIN
         gnosis_safe_gnosis.GnosisSafeL2_v1_3_0_evt_ExecutionSuccess t3
         ON
         t3.evt_tx_hash = t1.evt_tx_hash
         AND
         t3.evt_index = t1.evt_index + 2
    INNER JOIN
        circles_ubi_gnosis.Hub_evt_Trust t4
        ON
        t4.evt_tx_hash = t1.evt_tx_hash
        AND
        t4.evt_index = t1.evt_index + 1
        AND
        t4.canSendTo = t2.safe_wallet
    WHERE
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
        AND
        CAST(t1.gasPrice AS REAL) = 0 -- sponsored
        AND
        CAST(t3.payment AS REAL) = 0 -- sponsored
        
)

SELECT * FROM gnosis_metri_trust