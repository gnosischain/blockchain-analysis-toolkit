/*
======= Query Info =======                     
-- query_id: 3904777                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=from_date, value=2024-06-01 00:00:00, type=datetime), Parameter(name=to_date, value=2024-07-08 00:00:00, type=datetime)]                     
-- last update: 2024-07-25 17:22:52.733777                     
-- owner: hdser                     
==========================
*/


SELECT
    day
    ,blockchain
    ,COUNT(*) AS cnt
FROM (
    SELECT DISTINCT
        DATE_TRUNC('day',block_time) AS day
        ,blockchain
        ,"from" AS user
    FROM evms.transactions
    WHERE
        block_time >= TIMESTAMP '{{from_date}}'
        AND
        block_time <= TIMESTAMP '{{to_date}}'
)
GROUP BY 1, 2