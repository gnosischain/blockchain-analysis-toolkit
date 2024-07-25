/*
======= Query Info =======                 
-- query_id: 3785418                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.293356                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_metri_wallets AS (
    SELECT
         safe_wallet
        ,method
        ,created_at
        ,MIN(imported_at) AS imported_at
    FROM query_3674206
    WHERE 
        (created_at >= DATE '2024-05-01' AND method = 'Created')
        OR
        (imported_at >= DATE '2024-05-01' AND method = 'Imported')
    GROUP BY
    1,2,3
),

metri_counts_sparse AS (
    SELECT
        CAST(DATE_TRUNC('day',COALESCE(imported_at,created_at)) AS DATE) AS day
        ,method
        ,COUNT(*) AS cnt
    FROM
        gnosis_metri_wallets
    GROUP BY
        1,2
),

calendar AS (
    SELECT
        day
        ,method
    FROM (
        SELECT
            MIN(day) AS min_day
        FROM
            metri_counts_sparse
    )
    ,UNNEST(SEQUENCE(min_day, CURRENT_DATE - INTERVAL '1' DAY, INTERVAL '1' DAY)) s(day)
    ,UNNEST(ARRAY['Created','Imported']) t(method)
),

metri_counts_dense As (
    SELECT 
        t1.day
        ,t1.method
        ,COALESCE(t2.cnt,0) AS cnt
    FROM calendar t1
    LEFT JOIN
        metri_counts_sparse t2
        ON
        t2.day = t1.day
        AND
        t2.method = t1.method
)

SELECT 
    day
    ,method
    ,cnt
    ,SUM(cnt) OVER (PARTITION BY method ORDER BY day) AS total
    ,SUM(IF(method='Created',cnt,0)) OVER (ORDER BY day) AS total_created
    ,SUM(IF(method='Imported',cnt,0)) OVER (ORDER BY day) AS total_imported
    ,SUM(cnt) OVER (ORDER BY day) AS total_full
FROM metri_counts_dense
ORDER BY
    day DESC