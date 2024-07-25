/*
======= Query Info =======                     
-- query_id: 3502815                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.613316                     
-- owner: hdser                     
==========================
*/

SELECT
    number
FROM
  gnosis.blocks
WHERE
   time >= NOW() - INTERVAL '1' DAY
ORDER BY 
    number DESC
LIMIT 1