/*
======= Query Info =======                     
-- query_id: 3925763                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=token address, value=default value, type=text)]                     
-- last update: 2024-07-25 17:22:52.540681                     
-- owner: hdser                     
==========================
*/

WITH


volume_raw AS (
    SELECT
        evt_block_date AS block_day
        ,contract_address AS token_address
        ,SUM(value) AS amount_raw
    FROM
        erc20_gnosis.evt_transfer
    WHERE
        evt_block_date >= DATE '2023-01-01'
        AND
        evt_block_date < DATE '2024-07-01'
        AND
        value > 0
        AND
        contract_address = CAST({{token address}} AS varbinary)
    GROUP BY 1, 2
),

volume AS (
        SELECT 
            t1.block_day
            ,t1.token_address
            ,t2.symbol
            ,t1.amount_raw/POWER(10,t2.decimals) AS amount
        FROM volume_raw t1
        LEFT JOIN
            tokens.erc20 t2
            ON t2.blockchain = 'gnosis'
            AND
            t2.contract_address = t1.token_address
)


SELECT 
    CONCAT(CAST(YEAR(block_day) AS VARCHAR),' - Q', CAST(QUARTER(block_day) AS VARCHAR)) AS block_date
    ,token_address
    ,symbol
    ,COALESCE(SUM(amount),0) AS amount
FROM volume
GROUP BY 1, 2, 3



