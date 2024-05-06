-- query_id: 3629703

WITH

P256SignerFactory_NewSignerCreated AS (
    SELECT 
        VARBINARY_SUBSTRING(data,13,20) AS signer
    FROM 
        gnosis.logs
    WHERE
        contract_address = 0x73dA77F0f2daaa88b908413495d3D0e37458212e
        AND
        block_time >= DATE '2024-01-01'
),

Safev1_4_1_evt_SafeSetup AS (
    SELECT
        contract_address
        ,tx_hash AS evt_tx_hash
        ,tx_from AS evt_tx_from
        ,tx_to AS evt_tx_to
        ,index AS evt_index
        ,block_time AS evt_block_time
        ,block_number AS evt_block_number
        ,block_date AS evt_block_date
        ,varbinary_substring(topic1,13,20) AS initiator
        ,TRANSFORM(
            SEQUENCE(0,CAST(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1 + 4*32,32))) AS INTEGER)-1),
            x -> varbinary_substring(data,1 + (5+x)*32 + 12,20)
            ) AS owners
        ,varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1 + 32,32))) AS threshold
        ,varbinary_substring(data,1 + 2*32 + 12,20) AS initializer
        ,varbinary_substring(data,1 + 3*32 + 12,20) AS fallbackHandler
    FROM
        gnosis.logs
    WHERE
        block_time >= DATE '2023-05-30' 
        AND
        topic0 = 0x141df868a6331af528e38c83b7aa03edc19be66e37ae67f9285bf4f8e3c6a1a8 --SafeSetup
)

SELECT 
    t1.* 
    ,ARRAY_AGG(IF(CONTAINS(owners,t2.signer),1,0)) AS is_p256_signers
FROM 
    Safev1_4_1_evt_SafeSetup t1
CROSS JOIN
    P256SignerFactory_NewSignerCreated t2
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13
