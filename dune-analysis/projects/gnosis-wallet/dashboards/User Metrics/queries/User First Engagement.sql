-- query_id: 3680348

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM dune.hdser.query_3663810
),

gnosis_gw_wallets AS (
    SELECT * FROM dune.hdser.query_3674206
),

user_transactions AS (
    SELECT
        CASE
            WHEN t2.created_at < CURRENT_DATE - INTERVAL '30' DAY THEN 'Older'
            ELSE 'Last 30 Days'
        END AS label
        ,t1.to AS safe_wallet
        ,MIN(DATE_DIFF('hour', t2.created_at, t1.block_time)) AS min_time_diff
    FROM
        gnosis.transactions t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.to
   WHERE
        t1.block_time >= t2.created_at
    GROUP BY 1,2
),

distribution AS (
    SELECT
        label
        ,min_time_diff
        ,COUNT(DISTINCT safe_wallet) AS cnt
    FROM
        user_transactions
    GROUP BY 1, 2
),

norm_distribution AS (
    SELECT
        label
        ,min_time_diff AS time_diff
        ,cnt
        ,CAST(cnt AS REAL)/(SUM(cnt) OVER (PARTITION BY label)) AS pct
    FROM
        distribution
)


SELECT * FROM norm_distribution
WHERE time_diff < 48
