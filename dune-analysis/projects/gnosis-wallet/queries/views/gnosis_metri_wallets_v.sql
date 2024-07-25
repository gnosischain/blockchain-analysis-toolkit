/*
======= Query Info =======                 
-- query_id: 3674206                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:46.736621                 
-- owner: hdser                 
==========================
*/

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
