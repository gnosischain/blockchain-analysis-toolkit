-- query_id: 3674224

WITH

GnosisSafe_AddedOwner AS (
    SELECT * FROM query_3663783
),

gnosis_SafeSetup AS (
    SELECT * FROM query_3629703
),

gnosis_SafeMultiSigTransaction AS (
    SELECT * FROM gnosis_safe_gnosis.GnosisSafeL2_v1_3_0_evt_SafeMultiSigTransaction
),

gnosis_LogUseGelato1BalanceV2 AS (
    SELECT * FROM gnosis.logs
    WHERE contract_address = 0xf9d64d54d32ee2bdceaabfa60c4c438e224427d0
    AND
    topic0 = 0x8e4f8b7f1299a63a6b46587ec357933d2006e5697cd46d99297e670cee1dbeb1
    --gelato_gnosis.GelatoRelay_evt_LogUseGelato1BalanceV2
)


SELECT 
    t1.contract_address AS safe_wallet
    ,t1.evt_block_time AS created_at
    ,t2.evt_block_time AS imported_at
    ,t2.evt_tx_hash AS tx_hash_added
    ,t1.owners
    ,t2.owner AS new_owner_added
    ,'Imported' AS method
    ,t2.evt_tx_from
    ,t2.evt_tx_to
FROM
    gnosis_SafeSetup t1
INNER JOIN
    GnosisSafe_AddedOwner t2
    ON
    t2.contract_address = t1.contract_address
INNER JOIN
    gnosis_SafeMultiSigTransaction t3
    ON
    t3.evt_tx_hash = t2.evt_tx_hash
INNER JOIN
    gnosis_LogUseGelato1BalanceV2 t4
    ON
    t4.tx_hash = t2.evt_tx_hash