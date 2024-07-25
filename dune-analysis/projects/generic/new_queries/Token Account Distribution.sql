/*
======= Query Info =======                     
-- query_id: 3529068                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=token, value=EURe, type=enum)]                     
-- last update: 2024-07-25 17:22:54.259067                     
-- owner: hdser                     
==========================
*/


WITH

labels AS (
SELECT * FROM query_2418768
)

SELECT 
    t1.* 
    ,CONCAT(COALESCE(t2.name,''),' ', CAST(t1.address AS VARCHAR)) AS label
FROM 
    dune.hdser.result_token_current_distribution t1
LEFT JOIN
    labels t2
    ON t2.address = t1.address
WHERE
    symbol = '{{token}}'
    