/*
======= Query Info =======                 
-- query_id: 3800348                 
-- description: "This query tracks the daily circulating supply of ERC20 tokens on Gnosis Chain."                 
-- tags: ['ERC20', 'Supply', 'View']                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:47.970160                 
-- owner: hdser                 
==========================
*/

SELECT
    t1.token_address
    ,t1.block_day
    ,ABS(SUM(t1.amount_raw) OVER (PARTITION BY t1.token_address ORDER BY t1.block_day))/POWER(10,COALESCE(t2.decimals,18)) AS amount
FROM (
        SELECT 
            token_address
            ,block_day
            ,SUM(amount_raw) AS amount_raw
        FROM test_schema.git_dunesql_6849743_transfers_gnosis_erc20_agg_day
        WHERE 
            wallet_address = 0x0000000000000000000000000000000000000000
        GROUP BY
            1, 2
) t1
LEFT JOIN
    tokens.erc20 t2
    ON 
    t2.blockchain = 'gnosis'
    AND
    t2.contract_address = t1.token_address