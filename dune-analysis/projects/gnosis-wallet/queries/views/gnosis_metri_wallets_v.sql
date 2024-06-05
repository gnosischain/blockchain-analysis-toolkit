-- query_id: 3674206

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM query_3663810
),

gnosis_gw_comethImport AS (
    SELECT * FROM query_3674224
)

SELECT
    safe_wallet
    ,created_at
    ,NULL AS imported_at
    ,owners
    ,NULL AS new_owner_added
    ,method
    ,tx_hash_created AS tx_hash
FROM gnosis_gw_signupPerson
UNION ALL
SELECT
    safe_wallet
    ,created_at
    ,imported_at
    ,owners
    ,new_owner_added
    ,method
    ,tx_hash_added AS tx_hash
FROM gnosis_gw_comethImport
