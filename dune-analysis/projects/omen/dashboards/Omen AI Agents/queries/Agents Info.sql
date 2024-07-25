/*
======= Query Info =======                 
-- query_id: 3644125                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:45.130632                 
-- owner: hdser                 
==========================
*/

SELECT
  label AS "Label",
  GET_HREF(
    GET_CHAIN_EXPLORER_ADDRESS('gnosis', TRY_CAST(address AS VARCHAR)),
    TRY_CAST(address AS VARCHAR)
  ) AS "Address"
FROM query_3582994