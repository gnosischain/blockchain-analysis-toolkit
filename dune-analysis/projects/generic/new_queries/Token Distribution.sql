/*
======= Query Info =======                     
-- query_id: 3528831                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=token, value=EURe, type=enum)]                     
-- last update: 2024-07-25 17:22:49.738853                     
-- owner: hdser                     
==========================
*/

SELECT
*
FROM
    dune.hdser.result_tokens_supply_distribution
WHERE
    symbol = '{{token}}'
    AND
    day < DATE_TRUNC('day', NOW())
ORDER BY day DESC