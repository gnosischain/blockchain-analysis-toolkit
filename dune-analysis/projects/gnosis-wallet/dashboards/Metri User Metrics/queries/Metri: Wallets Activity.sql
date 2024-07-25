/*
======= Query Info =======                 
-- query_id: 3674148                 
-- description: "New wallets are both Created and Imported ones. "                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.906832                 
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

metri_new_wallets_sparse AS (
    SELECT
        CAST(DATE_TRUNC('day',COALESCE(imported_at,created_at)) AS DATE) AS day
        ,COUNT(*) AS cnt
    FROM
        gnosis_metri_wallets
    GROUP BY
        1
),


metri_returning_wallets_sparse AS (
    SELECT
        block_date_lead AS day
        ,COUNT(wallet_address) AS cnt
    FROM 
        query_3821724 --gnosis_wallet_address_appearances
    WHERE
        status = 'active'
    GROUP BY 1
),

calendar AS (
    SELECT
        day
    FROM (
        SELECT
            MIN(day) AS min_day
        FROM
            metri_new_wallets_sparse
    )
    ,UNNEST(SEQUENCE(min_day, CURRENT_DATE - INTERVAL '1' DAY, INTERVAL '1' DAY)) s(day)
)


SELECT 
    t1.day
    ,COALESCE(t2.cnt,0) AS "New"
    ,COALESCE(t3.cnt,0) AS "Returning"
    ,SUM(COALESCE(t2.cnt,0)) OVER (ORDER BY t1.day) AS "Total"
FROM 
    calendar t1
LEFT JOIN
    metri_new_wallets_sparse t2
    ON
    t2.day = t1.day
LEFT JOIN
    metri_returning_wallets_sparse t3
    ON
    t3.day = t1.day