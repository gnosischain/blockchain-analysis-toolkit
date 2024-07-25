/*
======= Query Info =======                 
-- query_id: 3748910                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.642544                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gw_comethImport AS (
    SELECT 
        safe_wallet
        ,MIN(DATE_TRUNC('day',imported_at)) AS day
    FROM query_3674224
    GROUP BY 1
)

SELECT
    day
    ,cnt AS "Count"
    ,SUM(cnt) OVER (ORDER BY day) as "Total"
FROM (
    SELECT 
        day
        ,COUNT(*) AS cnt
    FROM
        gnosis_gw_comethImport
    GROUP BY 1
)