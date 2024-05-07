-- query_id: 3680579

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM dune.hdser.query_3663810
    WHERE created_at >= DATE '2024-05-01'
),

minmax_dates AS (
    SELECT
        CAST(MIN(DATE_TRUNC('month',created_at)) AS DATE) AS start_day
        ,CURRENT_DATE AS end_day
    FROM 
        gnosis_gw_signupPerson
),

calendar AS (
    SELECT 
        day
    FROM
        minmax_dates t1
        ,UNNEST(SEQUENCE(t1.start_day, t1.end_day)) AS s(day)
),


user_transactions AS (
    SELECT
        t1.block_time
        ,t1.to AS safe_wallet
    FROM
        gnosis.transactions t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.to
   WHERE
        t1.block_time >= t2.created_at
),

daily_user_transactions AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day
        ,COUNT(DISTINCT safe_wallet) AS daily_cnt
    FROM
        user_transactions
    GROUP BY 1
),

weekly_user_transactions AS (
    SELECT
        DATE_TRUNC('week', block_time) AS week
        ,COUNT(DISTINCT safe_wallet) AS weekly_cnt
    FROM
        user_transactions
    GROUP BY 1
),

monthly_user_transactions AS (
    SELECT
        DATE_TRUNC('month', block_time) AS month
        ,COUNT(DISTINCT safe_wallet) AS monthly_cnt
    FROM
        user_transactions
    GROUP BY 1
)


SELECT
     t1.day
    ,COALESCE(t2.daily_cnt,0) AS daily
    ,t3.weekly_cnt AS weekly
    ,t4.monthly_cnt AS monthly
FROM calendar t1
LEFT JOIN
    daily_user_transactions t2
    ON
    t2.day = t1.day
LEFT JOIN
    weekly_user_transactions t3
    ON
    t3.week = t1.day
LEFT JOIN
    monthly_user_transactions t4
    ON
    t4.month = t1.day
