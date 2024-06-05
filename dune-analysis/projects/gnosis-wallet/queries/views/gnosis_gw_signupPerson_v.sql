-- query_id: 3663810

WITH 
/*
P256SignerFactory_NewSignerCreated AS (
  SELECT
    VARBINARY_SUBSTRING(data, 13, 20) AS signer
  FROM gnosis.logs
  WHERE
    contract_address = CAST('73dA77F0f2daaa88b908413495d3D0e37458212e' AS varbinary)
    AND block_time >= TRY_CAST('2024-01-01' AS timestamp)
),
*/
P256SignerFactory_NewSignerCreated AS (
  SELECT
    signer
  FROM cometh_gnosis.P256SignerFactory_evt_NewSignerCreated  
  WHERE
    evt_block_time >= TRY_CAST('2024-01-01' AS timestamp)
),


wallet_Safev1_4_1_evt_SafeSetup AS (
  SELECT
    t1.*
    ,IF(t2.signer IS NULL, 0, 1) AS has_p256_signer
  FROM dune.hdser.query_3629703 t1
  LEFT JOIN
    P256SignerFactory_NewSignerCreated t2
    ON
    t2.signer = t1.owners[1]
  WHERE
    CARDINALITY(t1.owners) = 1 AND t1.threshold = CAST(1 AS uint256)
), 

Hub_evt_Signup AS (
  SELECT
    *
  FROM circles_ubi_gnosis.Hub_evt_Signup
)


SELECT
  t1.contract_address AS safe_wallet
  ,t1.evt_block_time AS created_at
  ,t1.owners
  ,t1.has_p256_signer
  ,'Created' AS method
  ,t1.evt_tx_from
  ,t1.evt_tx_to
  ,t1.evt_tx_hash AS tx_hash_created
FROM 
    wallet_Safev1_4_1_evt_SafeSetup t1
INNER JOIN UNNEST(t1.owners) WITH ORDINALITY AS o(address, idx)
    ON TRUE
INNER JOIN 
    Hub_evt_Signup t2
    ON t2.evt_tx_hash = t1.evt_tx_hash 
    AND t2.user = t1.contract_address