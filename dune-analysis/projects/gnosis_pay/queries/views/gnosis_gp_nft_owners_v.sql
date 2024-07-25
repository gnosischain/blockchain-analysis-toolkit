/*
======= Query Info =======                     
-- query_id: 3920744                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=timestamp, value=2024-07-14 18:00:00, type=datetime)]                     
-- last update: 2024-07-25 17:22:54.193931                     
-- owner: hdser                     
==========================
*/

SELECT
     VARBINARY_SUBSTRING(topic2, 13, 20) AS address
FROM
    gnosis.logs
WHERE
    topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    AND
    contract_address = 0x88997988a6A5aAF29BA973d298D276FE75fb69ab
    AND
    block_time >= TIMESTAMP '2024-02-01 00:00'
    AND
    block_time <= TIMESTAMP '{{timestamp}}'