/*
======= Query Info =======                     
-- query_id: 3870052                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.127324                     
-- owner: hdser                     
==========================
*/


SELECT
    block_date
    ,(SUM(amount_raw) OVER (ORDER BY block_date))/POWER(10,18) AS total
    ,amount_raw/POWER(10,18) AS mint
FROM (
    SELECT 
        block_date
        ,SUM(amount_raw) AS amount_raw
    FROM 
        query_3869915 --gnosis_circles_transfers_agg_day_v
    WHERE
        counterparty = 0x0000000000000000000000000000000000000000
    GROUP BY 1
)