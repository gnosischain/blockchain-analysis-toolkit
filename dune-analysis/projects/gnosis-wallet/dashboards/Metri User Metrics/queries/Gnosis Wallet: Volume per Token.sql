--query_id: 3736335

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
        --OR
        --(imported_at >= DATE '2024-05-01' AND method = 'Imported')
    GROUP BY
    1,2,3
),

transfers_gnosis AS (
    SELECT
        block_time
        ,transfer_type
        ,wallet_address
        ,token_address
        ,method
        ,SUM(amount_raw) AS amount_raw
    FROM (
    SELECT
        t1.evt_block_time AS block_time
        ,t1.transfer_type
        ,t1.wallet_address
        ,t1.token_address
        ,t2.method
        ,t1.amount_raw
    FROM
        test_schema.git_dunesql_11470a0_transfers_gnosis_erc20 t1
    INNER JOIN
        gnosis_metri_wallets t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
        
    UNION ALL
        
    SELECT
        t1.block_time
        ,t1.transfer_type
        ,t1.wallet_address
        ,t1.token_address
        ,t2.method
        ,SUM(t1.amount_raw) AS amount_raw
    FROM
        test_schema.git_dunesql_85ca863_transfers_gnosis_xdai t1
    INNER JOIN
        gnosis_metri_wallets t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
    GROUP BY 1,2,3,4,5
    )
    GROUP BY 1,2,3,4,5
)


SELECT 
*
FROM (
    SELECT 
        DATE_TRUNC('day',t1.block_time) AS block_day
        ,CASE
            WHEN t1.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 'xDAI'
            WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 THEN 'GBPe'
            ELSE t2.symbol
        END AS symbol
        ,SUM(
            CASE
                WHEN t1.token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 
                    THEN ABS(t1.amount_raw)/POWER(10,18)*1.25
                ELSE  ABS(t1.amount_raw)/POWER(10,t2.decimals)*t2.price
            END
        ) AS amount
    FROM transfers_gnosis t1
    LEFT JOIN
        prices.usd t2
        ON t2.blockchain = 'gnosis'
        AND t2.minute = DATE_TRUNC('day',t1.block_time)
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
WHERE 
    symbol IS NOT NULL
    AND
    block_day >= CURRENT_DATE - INTERVAL '1' YEAR
    AND
    block_day < CURRENT_DATE
