-- query_id: 3746189

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

balances_diff AS (
    SELECT 
         block_day
        ,token_address
        ,entity_id
        ,SUM(amount_raw) AS amount_raw
    FROM (
    SELECT
        t1.block_day
        ,t1.token_address
        ,t2.entity_id
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_11470a0_transfers_gnosis_erc20_agg_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.pay_wallet = t1.wallet_address
    GROUP BY 1,2,3
    
    UNION ALL
    
    SELECT
        t1.block_day
        ,t1.token_address
        ,t2.entity_id
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_11470a0_transfers_gnosis_erc20_agg_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.owner = t1.wallet_address
    GROUP BY 1,2,3
    )
    GROUP BY 1,2,3
    
),

calendar AS (
    SELECT 
        token_address
        ,entity_id
        ,block_day
    FROM (
        SELECT 
            t1.token_address
            ,t1.entity_id
            ,MIN(t1.block_day) AS block_day_min
        FROM
            balances_diff t1
        GROUP BY 1,2
    )
    ,UNNEST(SEQUENCE(CAST(block_day_min AS DATE),CURRENT_DATE, INTERVAL '1' DAY)) s(block_day)
),

balances_diff_dense AS (
    SELECT 
        t2.block_day
        ,t2.token_address
        ,t2.entity_id
        ,COALESCE(t1.amount_raw,0) AS amount_raw
    FROM
        balances_diff t1
    RIGHT JOIN
        calendar t2
        ON
        t2.block_day = t1.block_day
        AND 
        t2.token_address = t1.token_address
        AND 
        t2.entity_id = t1.entity_id
),

balances_entities AS (
        SELECT 
            t1.block_day
            ,entity_id
            ,SUM(
                CASE
                    WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 
                        THEN t1.amount_raw/POWER(10,18)*1.25
                    ELSE  t1.amount_raw/POWER(10,t2.decimals)*t2.price
                END
            ) AS amount
        FROM (
            SELECT
             block_day
            ,token_address
            ,entity_id
            ,SUM(amount_raw) OVER (PARTITION BY entity_id,token_address ORDER BY block_day) AS amount_raw
            FROM
                balances_diff_dense 
        ) t1
        LEFT JOIN
            prices.usd t2
            ON t2.blockchain = 'gnosis'
            AND t2.minute = t1.block_day
            AND
            (
            t2.contract_address = t1.token_address
            OR 
            (t2.contract_address = 0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d 
                AND 
            t1.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)
            )
        GROUP BY 1,2
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
    block_day
    ,cohort
    ,SUM(amount) AS amount
    ,COUNT(*) AS entities_cnt
FROM
    (
SELECT 
    t1.block_day
   ,t2.cohort
   ,t1.amount
FROM balances_entities t1
CROSS JOIN
    size_cohorts t2
WHERE
    t1.amount >= t2.lower AND t1.amount < t2.upper
    AND 
    block_day >= CURRENT_DATE - INTERVAL '1' YEAR
    AND
    block_day < CURRENT_DATE
  )
 GROUp BY 1,2