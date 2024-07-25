/*
======= Query Info =======                     
-- query_id: 3510938                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.254598                     
-- owner: hdser                     
==========================
*/

SELECT
  COUNT(CASE WHEN success = TRUE THEN 1 END) AS "Success"
FROM gnosis.transactions
WHERE
  block_time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
