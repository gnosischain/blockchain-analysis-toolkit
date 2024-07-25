/*
======= Query Info =======                     
-- query_id: 3511336                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.450164                     
-- owner: hdser                     
==========================
*/

WITH
supply_mint AS (
    SELECT 
        SUM(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))/POWER(10,18)) AS value
    FROM 
        gnosis.logs
    WHERE 
        contract_address = 0x9c58bacc331c9aa871afd802db6379a98e80cedb 
        AND
        topic0 = 0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885
),

supply_burn AS (
    SELECT 
        -SUM(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))/POWER(10,18)) AS value
    FROM 
        gnosis.logs
    WHERE 
        contract_address = 0x9c58bacc331c9aa871afd802db6379a98e80cedb 
        AND
        topic0 = 0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5
)

SELECT
    SUM(value) AS total
FROM (
    SELECT 
        * 
    FROM
        supply_mint
    UNION ALL
    SELECT 
        *
    FROM
        supply_burn 
)