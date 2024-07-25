/*
======= Query Info =======                     
-- query_id: 3732061                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=token_address, value=0x5Cb9073902F2035222B9749F8fB0c9BFe5527108, type=enum)]                     
-- last update: 2024-07-25 17:22:49.134177                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_gp_wallet_flows_balance AS (
    SELECT * FROM query_3713262
    WHERE token_address = CAST({{token_address}} AS varbinary)
    AND action = 'Balance'
),

entity_id_dates AS (
    SELECT 
        entity_id
        ,MIN(evt_block_date) AS block_date
    FROM gnosis_gp_wallet_flows_balance
    GROUP BY 1
),

calendar AS (
    SELECT 
        entity_id
        ,day
    FROM
        entity_id_dates
        ,UNNEST(SEQUENCE(CAST(block_date AS DATE), CURRENT_DATE, INTERVAL '1' DAY)) s(day)
),

balance_dense AS (
    SELECT
        day
       ,entity_id
       ,LAST_VALUE(value/POWER(10,COALESCE(t3.decimals,18))) IGNORE NULLS OVER (PARTITION BY entity_id ORDER BY day) AS value
    FROM (
    SELECT
       t2.day
       ,t2.entity_id
       ,t1.value
    FROM 
        gnosis_gp_wallet_flows_balance t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.evt_block_date
        AND
        t2.entity_id = t1.entity_id
     ) 
     LEFT JOIN
    tokens.erc20 t3
    ON 
    t3.contract_address = CAST({{token_address}} AS varbinary)
),

size_cohorts AS (
    SELECT
        lower
        ,upper
        
        ,IF(upper=1e20,
            '1M>=',
            CASE
                WHEN upper = 10 THEN '0-10'
                WHEN upper = 100 THEN '10-100'
                WHEN upper = 1e3 THEN '100-1K'
                WHEN upper = 1e4 THEN '1K-10K'
                WHEN upper = 1e5 THEN '10K-100K'
                WHEN upper = 1e6 THEN '100K-1M'
            END
            ) AS cohort
    FROM
        UNNEST(ARRAY[0,10,1e2,1e3,1e4,1e5,1e6]) WITH ORDINALITY m(lower,idx)
        ,UNNEST(ARRAY[10,1e2,1e3,1e4,1e5,1e6,1e20]) WITH ORDINALITY s(upper,idx)
    WHERE
        m.idx = s.idx
)

SELECT
    day
    ,cohort
    ,SUM(value) AS value
    ,COUNT(*) AS entities_cnt
FROM
    (
SELECT 
    t1.day
   ,t2.cohort
   ,t1.value
FROM balance_dense t1
CROSS JOIN
    size_cohorts t2
WHERE
    t1.value >= t2.lower AND t1.value < t2.upper
  )
 GROUp BY 1,2