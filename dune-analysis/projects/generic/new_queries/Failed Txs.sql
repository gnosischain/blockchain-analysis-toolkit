/*
======= Query Info =======                     
-- query_id: 3510953                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.587754                     
-- owner: hdser                     
==========================
*/

SELECT
  COUNT(CASE WHEN success = FALSE THEN 1 END) AS "Fail"
FROM gnosis.transactions
WHERE
  block_time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR