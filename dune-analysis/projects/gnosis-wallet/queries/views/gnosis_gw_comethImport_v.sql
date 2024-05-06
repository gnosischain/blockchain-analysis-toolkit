-- query_id: 3674224

WITH

GnosisSafe_AddedOwner AS (
    SELECT * FROM dune.hdser.query_3663783
),

gnosis_SafeSetup AS (
    SELECT * FROM dune.hdser.query_3629703
)

SELECT 
    t1.contract_address AS safe_wallet
    ,t1.evt_block_time AS created_at
    ,t2.evt_block_time AS imported_at
    ,t2.evt_tx_hash
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

