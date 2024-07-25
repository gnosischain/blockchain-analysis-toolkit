/*
======= Query Info =======                     
-- query_id: 3510922                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.876558                     
-- owner: hdser                     
==========================
*/

SELECT
  DATE_TRUNC('day', block_time) AS day,
  COUNT(CASE WHEN success = TRUE THEN 1 END) AS "Success",
  COUNT(CASE WHEN success = FALSE THEN 1 END) AS "Fail",
  COUNT(*) AS "total"
FROM gnosis.transactions
WHERE
    block_time > NOW() - INTERVAL '3' YEAR
GROUP BY
  1