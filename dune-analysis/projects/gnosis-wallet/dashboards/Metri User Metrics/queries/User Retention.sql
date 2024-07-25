/*
======= Query Info =======                 
-- query_id: 3699769                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.837874                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM query_3663810
    WHERE created_at >= DATE '2024-05-01'
),

user_transactions AS (
    SELECT
        t1.to AS safe_wallet
        ,CAST(DATE_TRUNC('week',t1.block_time) AS DATE) AS date
    FROM
        gnosis.transactions t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.to
   WHERE
        t1.block_time >= t2.created_at
    GROUP BY 1, 2
),

gw_weekly_signups AS (
    SELECT
        CAST(DATE_TRUNC('week', created_at) AS DATE) AS date
        ,safe_wallet AS new_user
    FROM
        gnosis_gw_signupPerson
),

gw_weekly_users_total AS (
    SELECT
        date
        ,new_users
        ,total_users
        ,LAG(total_users) OVER (ORDER BY date) AS total_users_lag
    FROM (
        SELECT
            date
            ,new_users
            ,SUM(new_users) OVER (ORDER BY date) AS total_users
        FROM (
            SELECT
                date
                ,COUNT(*) AS new_users
            FROM
                gw_weekly_signups
            GROUp BY 1
        )
    )
),

weekly_returning_users AS (
    SELECT 
        t1.safe_wallet AS returning_user
        ,t1.date
        ,COUNT(*) AS cnt_txs
    FROM user_transactions t1
    LEFT JOIN
        gw_weekly_signups t2
        ON
        t2.date = t1.date
    WHERE
        t1.safe_wallet != t2.new_user
    GROUP BY 1, 2
),

calendar AS (
    SELECT
        date
    FROM (
        SELECT 
            MIN(date) AS min_date
            ,CAST(DATE_TRUNC('week',NOW()) AS DATE) AS max_date
        FROM
            weekly_returning_users
    ) t1
    ,UNNEST(SEQUENCE(t1.min_date, t1.max_date,INTERVAL '7' DAY)) AS s(date)
)


SELECT
    t3.date
    ,COALESCE(CAST(cnt_returning_users AS REAL)/total_users_lag,0) AS "Returning Rate"
    ,1 - COALESCE(CAST(cnt_returning_users AS REAL)/total_users_lag,0) AS "Churn Rate"
    ,COALESCE(cnt_txs,0) AS cnt_txs
FROM (
    SELECT 
        date
        ,COUNT(*) AS cnt_returning_users
        ,SUM(cnt_txs) AS cnt_txs
    FROM
        weekly_returning_users 
    GROUP BY 1
) t1
LEFT JOIN
    gw_weekly_users_total t2
    ON
    t2.date = t1.date
RIGHT JOIN
    calendar t3
    ON
    t3.date = t1.date
