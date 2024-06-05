-- query_id: 3680348

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM query_3663810
    WHERE created_at >= DATE '2024-05-01'
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
),

hours_from_creation AS (
    SELECT
        label
        ,time_diff
    FROM
        UNNEST(SEQUENCE(0,47)) s(time_diff)
        ,UNNEST(ARRAY['Older', 'Last 30 Days']) s(label)
)


SELECT
    t2.label
    ,t2.time_diff
    ,COALESCE(t1.cnt,0) AS cnt
    ,COALESCE(t1.pct,0) AS pct
FROM norm_distribution t1
RIGHT JOIN
    hours_from_creation t2
    ON
    t2.time_diff = t1.time_diff
    AND
    t2.label = t1.label
