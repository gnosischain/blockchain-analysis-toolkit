/*
======= Query Info =======                     
-- query_id: 3921132                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.803350                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_gp_users AS (
    SELECT 
        pay_wallet 
        ,owner
    FROM query_3707804
),

approvals AS (
    SELECT 
        t1.evt_block_time
        ,t2.owner
        ,t2.pay_wallet
        ,t1.contract_address
        ,t1.value AS value_raw
        ,IF(t2.pay_wallet = t1.owner, 'Wallet', 'Owner') AS approval
    FROM
        erc20_gnosis.evt_approval t1
    INNER JOIN
        gnosis_gp_users t2
        ON
        t2.pay_wallet = t1.owner
        OR
        t2.owner = t1.owner
    WHERE 
        t1.spender = 0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae
)

SELECT 
    owner
    ,pay_wallet
    ,contract_address
    ,approval
    ,ARRAY_AGG(value_raw ORDER BY evt_block_time DESC)[1] AS value_raw
FROM approvals
GROUP BY 1, 2, 3, 4

    