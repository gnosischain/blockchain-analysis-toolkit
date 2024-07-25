/*
======= Query Info =======                     
-- query_id: 3511301                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:48.106548                     
-- owner: hdser                     
==========================
*/

WITH
supply_mint AS (
    SELECT 
        DATE_TRUNC('day', block_time) AS day
        ,SUM(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))/POWER(10,18)) AS value
    FROM 
        gnosis.logs
    WHERE 
        contract_address = 0x9c58bacc331c9aa871afd802db6379a98e80cedb 
        AND
        topic0 = 0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885
    GROUP BY
        1
),

supply_burn AS (
-- Use transfer to count possible direct burns
    SELECT 
        DATE_TRUNC('day', block_time) AS day
        ,SUM(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,1,32)))/POWER(10,18)) AS value
    FROM 
        gnosis.logs
    WHERE 
        contract_address = 0x9c58bacc331c9aa871afd802db6379a98e80cedb 
        AND
        topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        AND
        topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1
),

block_days AS (
    SELECT
         DATE_TRUNC('day', time) AS day
    FROM
        gnosis.blocks
    GROUP BY 
        1
),

supply_net AS (
    SELECT 
        d.day
        ,COALESCE(m.value,0)  AS mint
        ,COALESCE(b.value,0) AS burn
        ,COALESCE(m.value,0) - COALESCE(b.value,0) AS net
    FROM
        block_days d
    LEFT JOIN
        supply_mint m
        ON
        m.day = d.day
    LEFT JOIN 
        supply_burn b
        ON
        b.day = d.day
),

final AS (
    SELECT 
        day
        ,mint
        ,burn
        ,net AS "Net Change"
        ,SUM(mint) OVER w AS cumsum_mint
        ,SUM(burn) OVER w AS cumsum_burn
        ,SUM(net) OVER w AS "Supply"
    FROM
        supply_net
    WINDOW w AS (
        ORDER BY day
    )
)

SELECT * FROM final 
WHERE "Supply" > 0