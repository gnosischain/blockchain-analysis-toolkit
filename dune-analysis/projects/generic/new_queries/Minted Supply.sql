/*
======= Query Info =======                     
-- query_id: 3511073                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:53.197592                     
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
    topic0 = 0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885
    AND
    block_time > NOW() - INTERVAL '24' HOUR


