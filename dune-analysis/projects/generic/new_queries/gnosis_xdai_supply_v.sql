/*
======= Query Info =======                     
-- query_id: 3822830                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.932628                     
-- owner: hdser                     
==========================
*/

SELECT
    block_date
    ,(SUM(amount_raw) OVER (ORDER BY block_date))/POWER(10,18) AS value
FROM (
    SELECT 
        block_date
        ,SUM(amount_raw) AS amount_raw
    FROM test_schema.git_dunesql_8c6b2af_tokens_gnosis_transfers
    --tokens_gnosis.transfers
    WHERE token_standard  =  'native' AND "from" is NULL
    GROUP BY
        1
        
    UNION ALL
    
    SELECT 
        block_date
        ,-CAST(SUM(amount_raw) AS INT256) AS amount_raw
    FROM test_schema.git_dunesql_8c6b2af_tokens_gnosis_transfers
   -- tokens_gnosis.transfers
    WHERE 
        token_standard  =  'native' 
        AND (to = 0x0000000000000000000000000000000000000000 OR to IS NULL)
    GROUP BY 1
)
