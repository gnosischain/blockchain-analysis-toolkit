/*
======= Query Info =======                     
-- query_id: 3817121                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.390211                     
-- owner: hdser                     
==========================
*/

WITH

inscriptions AS (
    SELECT 
        CONCAT(CAST(YEAR(block_time) AS VARCHAR),' - Q', CAST(QUARTER(block_time) AS VARCHAR)) AS block_date
        ,COUNT(*) cnt
    FROM    
        inscription.all
    WHERE
        blockchain = 'gnosis'
        AND
        block_time >= DATE '2023-01-01'
    GROUP BY 1
),

total_txs AS (
SELECT 
    CONCAT(CAST(YEAR(t1.block_time) AS VARCHAR),' - Q', CAST(QUARTER(t1.block_time) AS VARCHAR)) AS block_date
    ,COUNT(*) As cnt
FROM 
    gnosis.transactions t1
WHERE 
    t1.block_time >= DATE '2023-01-01'
GROUP BY 1
)

SELECT
  t1.block_date
  ,t1.cnt - COALESCE(t2.cnt,0) AS standard
  ,COALESCE(t2.cnt,0) AS inscriptions
FROM 
    total_txs t1
LEFT JOIN
    inscriptions t2
    ON
    t2.block_date = t1.block_date