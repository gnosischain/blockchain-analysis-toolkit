/*
======= Query Info =======                 
-- query_id: 3663783                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.224816                 
-- owner: hdser                 
==========================
*/

WITH 

GnosisSafe_v1_1_1_evt_AddedOwner AS (
    SELECT
        *
        ,'v1.1.1' AS version
    FROM
        gnosis_safe_gnosis.GnosisSafe_v1_1_1_evt_AddedOwner
),

GnosisSafe_v1_1_1_circles_evt_AddedOwner AS (
    SELECT
        *
        ,'v1.1.1_circles' AS version
    FROM
        gnosis_safe_gnosis.GnosisSafe_v1_1_1_circles_evt_AddedOwner
),


GnosisSafe_v1_2_0_evt_AddedOwner AS (
    SELECT
        *
        ,'v1.2.0' AS version
    FROM
        gnosis_safe_gnosis.GnosisSafe_v1_2_0_evt_AddedOwner
),


GnosisSafeL2_v1_3_0_evt_AddedOwner AS (
    SELECT
        *
        ,'v1.3.0_L2' AS version
    FROM
        gnosis_safe_gnosis.GnosisSafeL2_v1_3_0_evt_AddedOwner
),

GnosisSafe_v1_3_0_evt_AddedOwner AS (
    SELECT
        *
        ,'v1.3.0' AS version
    FROM
        gnosis_safe_gnosis.GnosisSafe_v1_3_0_evt_AddedOwner
),

final AS (
    SELECT * FROM GnosisSafe_v1_1_1_evt_AddedOwner
    UNION ALL
    SELECT * FROM GnosisSafe_v1_1_1_circles_evt_AddedOwner
    UNION ALL
    SELECT * FROM GnosisSafe_v1_2_0_evt_AddedOwner
    UNION ALL
    SELECT * FROM GnosisSafeL2_v1_3_0_evt_AddedOwner
),

P256SignerFactory_NewSignerCreated AS (
  SELECT
    signer
  FROM cometh_gnosis.P256SignerFactory_evt_NewSignerCreated  
  WHERE
    evt_block_time >= TRY_CAST('2024-01-01' AS timestamp)
)

SELECT 
    t1.*
    ,IF(t2.signer IS NULL, 0, 1) AS is_p256_signer
FROM final t1
LEFT JOIN
    P256SignerFactory_NewSignerCreated t2
    ON
    t2.signer = t1.owner