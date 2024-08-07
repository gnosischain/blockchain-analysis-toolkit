/*
======= Query Info =======                 
-- query_id: 3680963                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.510696                 
-- owner: hdser                 
==========================
*/

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM query_3663810
    WHERE created_at >= DATE '2024-05-01'
),

final AS (
    SELECT 
        CASE
            WHEN created_at >= CURRENT_DATE - INTERVAL '1' MONTH THEN '< 1 Month'
            WHEN created_at >= CURRENT_DATE - INTERVAL '2' MONTH THEN '1 - 2 Months'
            WHEN created_at >= CURRENT_DATE - INTERVAL '3' MONTH THEN '2 - 3 Months'
        END age_cohort
        ,EXTRACT(HOUR FROM created_at) AS hour
        ,COUNT(*) AS cnt
    FROM gnosis_gw_signupPerson
    GROUP BY 1, 2
),

calendar AS (
    SELECT 
        age_cohort
        ,hour
    FROM 
        UNNEST(SEQUENCE(0,23)) AS s(hour)
    CROSS JOIN UNNEST(ARRAY['< 1 Month', '1 - 2 Months', '2 - 3 Months']) AS t(age_cohort)
)

SELECT 
    t2.age_cohort
    ,t2.hour
    ,COALESCE(t1.cnt,0) AS cnt
    ,CAST(COALESCE(t1.cnt,0) AS REAL)/NULLIF(SUM(COALESCE(t1.cnt,0)) OVER (PARTITION BY t2.age_cohort),0) AS pct
FROM
    final t1
RIGHT JOIN
    calendar t2
    ON 
    t2.hour = t1.hour
    AND
    t2.age_cohort = t1.age_cohort