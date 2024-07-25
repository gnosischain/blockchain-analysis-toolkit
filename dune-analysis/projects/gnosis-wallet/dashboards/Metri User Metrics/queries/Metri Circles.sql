/*
======= Query Info =======                 
-- query_id: 3792185                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.374130                 
-- owner: hdser                 
==========================
*/

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
        OR
        (imported_at >= DATE '2024-05-01' AND method = 'Imported')
    GROUP BY
    1,2,3
),

metri_wallets_Trust AS (
  SELECT
    DATE_TRUNC('DAY',t1.evt_block_time) AS block_day
    ,t2.method
    ,COUNT(t1.evt_index) AS trust_cnt
  FROM circles_ubi_gnosis.Hub_evt_Trust t1
  INNER JOIN
    gnosis_metri_wallets t2
    ON
    t2.safe_wallet = t1.canSendTo
    GROUP BY 
        1,2
),

circle_metadata AS (
    SELECT 
        token AS token_address
        ,'CRC' AS symbol
        ,18 AS decimals
    FROM circles_ubi_gnosis.Hub_evt_Signup
),

crc_transfers_gnosis AS (
    SELECT
        t1.evt_block_time AS block_time
        ,t1.wallet_address
        ,t2.method
        ,t1.transfer_type
        ,t1.amount_raw/POWER(10,18) AS amount
    FROM
        test_schema.git_dunesql_075f38f_transfers_gnosis_erc20 t1
    INNER JOIN
        gnosis_metri_wallets t2
        ON
        t2.safe_wallet = t1.wallet_address
        AND
        t1.evt_block_time >= DATE_TRUNC('day',COALESCE(t2.imported_at,t2.created_at))
     INNER JOIN
        circle_metadata t3
        ON
        t3.token_address = t1.token_address
),

metri_crc_volume AS (
    SELECT 
        DATE_TRUNC('day',block_time) AS block_day
        ,method
        ,SUM(IF(transfer_type = 'send',ABS(amount),0)) AS sent_volume
        ,SUM(IF(transfer_type = 'receive',amount,0)) AS received_volume
        ,SUM(IF(transfer_type = 'mint',amount,0)) AS mint_volume
        ,SUM(IF(transfer_type = 'burn',ABS(amount),0)) AS burn_volume
        ,SUM(IF(transfer_type = 'send',1,0)) AS sent_count
        ,SUM(IF(transfer_type = 'receive',1,0)) AS received_count
        ,SUM(IF(transfer_type = 'mint',1,0)) AS mint_count
        ,SUM(IF(transfer_type = 'burn',1,0)) AS burn_count
    FROM crc_transfers_gnosis 
    GROUP BY 
        1,2
)

SELECT 
    t1.*
    ,(mint_volume + received_volume) - (burn_volume + sent_volume) AS net_volume
    ,COALESCE(t2.trust_cnt,0) AS trust_cnt
FROM metri_crc_volume t1
LEFT JOIN
    metri_wallets_Trust t2
    ON
    t2.block_day = t1.block_day
    AND
    t2.method = t1.method