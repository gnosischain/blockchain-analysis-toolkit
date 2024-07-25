/*
======= Query Info =======                     
-- query_id: 3820993                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.295125                     
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

metri_crc_mint_evts AS (
    SELECT 
        t1.evt_tx_hash
        ,t1.evt_index
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
        gnosis.logs t4
        ON
        t4.tx_hash = t1.evt_tx_hash
        AND
        t4.index = t1.evt_index + 3
    WHERE
        -- LogUseGelato1BalanceV2
        t4.contract_address = 0xF9D64d54D32EE2BDceAAbFA60C4C438E224427d0
        AND
        t4.topic0 = 0x8e4f8b7f1299a63a6b46587ec357933d2006e5697cd46d99297e670cee1dbeb1
        AND
        t4.block_time >= DATE '2024-05-01' 
        AND
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
        AND
        CAST(t1.gasPrice AS REAL) = 0 -- sponsored
        AND
        CAST(t3.payment AS REAL) = 0 -- sponsored
),

metri_crc_setup_mint_evts AS (
    SELECT 
        t1.evt_tx_hash
        ,t1.evt_index
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
         t3.evt_index = t1.evt_index + 4
    INNER JOIN
        circles_ubi_gnosis.Hub_evt_Signup t4
        ON
        t4.evt_tx_hash = t1.evt_tx_hash
        AND
        t4.evt_index = t1.evt_index + 3
    INNER JOIN
        circles_ubi_gnosis.Hub_evt_Trust t5
        ON
        t5.evt_tx_hash = t1.evt_tx_hash
        AND
        t5.evt_index = t1.evt_index + 2
    WHERE
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
        AND
        CAST(t1.gasPrice AS REAL) = 0 -- sponsored
        AND
        CAST(t3.payment AS REAL) = 0 -- sponsored
),

metri_crc_all_mint_evts AS (
    SELECT * FROM metri_crc_setup_mint_evts
    UNION ALL
    SELECT * FROM metri_crc_mint_evts
),

circle_metadata AS (
    SELECT 
        token AS token_address
        ,'CRC' AS symbol
        ,18 AS decimals
    FROM circles_ubi_gnosis.Hub_evt_Signup
),

crc_transfers_gnosis AS (
    SELECT
         t1.evt_tx_hash
        ,t1.evt_block_time
        ,t1.evt_index
        ,t1.wallet_address
        ,t1.counterparty
        ,t1.token_address
        ,t2.method
        ,t1.transfer_type
        ,t1.amount_raw/POWER(10,18) AS amount
    FROM
        test_schema.git_dunesql_3334f50_transfers_gnosis_erc20 t1
    INNER JOIN
        gnosis_metri_wallets t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
     INNER JOIN
        circle_metadata t3
        ON
        t3.token_address = t1.token_address
    WHERE
        t1.counterparty = 0x0000000000000000000000000000000000000000
        AND
        t1.amount_raw  > 0 -- not really needed
)

SELECT
    t1.evt_tx_hash
    ,t1.evt_index
    ,t1.evt_block_time
    ,t1.token_address
    ,t1.wallet_address 
    ,t1.counterparty
    ,t1.amount
    ,t1.method
FROM
    crc_transfers_gnosis t1
INNER JOIN
    metri_crc_all_mint_evts t2
    ON
    t2.evt_tx_hash = t1.evt_tx_hash
    AND
    t2.evt_index = t1.evt_index - 1
