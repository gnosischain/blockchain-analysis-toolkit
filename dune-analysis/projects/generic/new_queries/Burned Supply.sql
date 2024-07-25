/*
======= Query Info =======                     
-- query_id: 3511093                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:53.460893                     
-- owner: hdser                     
==========================
*/

SELECT 
    SUM(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))/POWER(10,18)) AS value
FROM 
    gnosis.logs
WHERE 
    contract_address = 0x9c58bacc331c9aa871afd802db6379a98e80cedb 
    AND
    topic0 = 0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5
    AND
    block_time > NOW() - INTERVAL '24' HOUR