-- query_id: 3736387

WITH

gnosis_gw_signupPerson AS (
    SELECT * FROM dune.hdser.query_3663810
    WHERE created_at >= DATE '2024-05-01'
),

transfers_gnosis AS (
    SELECT
        t1.evt_block_time AS block_time
        ,t1.transfer_type
        ,t1.wallet_address
        ,t1.token_address
        ,t1.amount_raw
    FROM
        test_schema.git_dunesql_e0a3349_transfers_gnosis_erc20 t1
    INNER JOIN
        gnosis_gw_signupPerson t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.evt_block_time >= DATE_TRUNC('day',t2.created_at)
),


circle_metadata AS (
    SELECT 
        token AS token_address
        ,'CRC' AS symbol
        --,CONCAT('CRC_',CAST(user AS VARCHAR)) AS symbol
        ,18 AS decimals
    FROM circles_ubi_gnosis.Hub_evt_Signup
)

    SELECT 
        DATE_TRUNC('day',t1.block_time) AS block_day
        ,SUM(
           ABS(t1.amount_raw)/POWER(10,t2.decimals)
        ) AS CRC
    FROM transfers_gnosis t1
    INNER JOIN
        circle_metadata t2
        ON
        t2.token_address = t1.token_address
    GROUP BY 1