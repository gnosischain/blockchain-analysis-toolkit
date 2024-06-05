-- query_id: 3746283

WITH

gnosis_gp_users AS (
    SELECT * FROM query_3707804
),

balances_diff AS (
    SELECT
        t1.block_day
        ,t1.token_address
        ,'Wallet' AS label
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
        ,'Owner' AS label
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_11470a0_transfers_gnosis_erc20_agg_day t1
    INNER JOIN
        gnosis_gp_users t2
        ON 
        t2.owner = t1.wallet_address
    GROUP BY 1,2,3
    
),

calendar AS (
    SELECT 
        token_address
        ,label
        ,block_day
    FROM (
        SELECT 
            t1.token_address
            ,t1.label
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
        ,t2.label
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
        t2.label = t1.label
),

balances_entities AS (
        SELECT 
            t1.block_day
            ,label
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
            ,label
            ,SUM(amount_raw) OVER (PARTITION BY label,token_address ORDER BY block_day) AS amount_raw
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
)

SELECT
    block_day
    ,label
   ,t1.amount
FROM balances_entities t1
WHERE
    block_day >= CURRENT_DATE - INTERVAL '1' YEAR
    AND
    block_day < CURRENT_DATE