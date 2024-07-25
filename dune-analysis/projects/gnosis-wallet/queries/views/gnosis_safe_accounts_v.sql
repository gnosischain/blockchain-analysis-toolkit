/*
======= Query Info =======                 
-- query_id: 3629658                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.156818                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_safe AS (
    SELECT 
    tx_hash
    ,contract_address
    ,block_time
     ,CASE
        WHEN contract_address = 0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67
            THEN varbinary_substring(topic1,13,20)
        ELSE varbinary_substring(data,13,20)  
     END AS address
     ,CASE
        WHEN contract_address = 0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2 
            THEN varbinary_substring(data,45,20)
        WHEN contract_address = 0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67
            THEN varbinary_substring(data,13,20)
     END AS singleton
    FROM
        gnosis.logs
    WHERE
        (
            (
                contract_address = 0x12302fE9c02ff50939BaAaaf415fc226C078613C --ProxyFactory 1.0.0
                OR
                contract_address = 0x76E2cFc1F5Fa8F6a5b3fC4c8F4788F0116861F9B --ProxyFactory 1.1.1
            )
            AND
            topic0 = 0xa38789425dbeee0239e16ff2d2567e31720127fbc6430758c1a4efc6aef29f80 --ProxyCreation 
        )
        OR
        (
            (
                contract_address = 0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2 --ProxyFactory 1.3.0
                OR
                contract_address = 0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67 --ProxyFactory 1.4.1
            )
            AND
            topic0 = 0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235 --ProxyCreation 
        )
)

SELECT * FROM gnosis_safe