-- query_id: 3674148

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM dune.hdser.query_3663810
),

minmax_dates AS (
    SELECT
        CAST(MIN(DATE_TRUNC('day',created_at)) AS DATE) AS start_day
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

daily_creations AS (
    SELECT 
        CAST(DATE_TRUNC('day',created_at) AS DATE) AS day
        ,COUNT(DISTINCT safe_wallet) as cnt_wallets
    FROM 
        gnosis_gw_signupPerson
    GROUP BY 1
),  

daily_created_wallets AS (
    SELECT
        t2.day
        ,COALESCE(t1.cnt_wallets,0) AS wallets
        ,SUM(COALESCE(t1.cnt_wallets,0)) OVER (ORDER BY t2.day) AS cumulative
    FROM
        daily_creations t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.day
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
        ,safe_wallet
        ,COUNT(*) AS cnt
    FROM
        user_transactions
    GROUP BY 1, 2
),

first_appearance AS (
    SELECT
        safe_wallet,
        MIN(day) AS first_day
    FROM daily_user_transactions
    GROUP BY 1
), 

daily_returning_addresses AS (
    SELECT
        w.day,
        COUNT(DISTINCT CASE WHEN w.day > f.first_day THEN w.safe_wallet END) AS returning_addresses
    FROM 
        daily_user_transactions w
    JOIN first_appearance f 
        ON w.safe_wallet = f.safe_wallet
    GROUP BY w.day
)

SELECT
     t1.day
    ,t1.wallets AS "New Wallets"
    ,COALESCE(t2.returning_addresses,0) AS "Returning"
    ,t1.cumulative AS "Total Wallets"
FROM daily_created_wallets t1
LEFT JOIN
    daily_returning_addresses t2
    ON
    t2.day = t1.day