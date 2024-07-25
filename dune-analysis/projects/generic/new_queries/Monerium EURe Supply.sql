/*
======= Query Info =======                     
-- query_id: 3519887                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:55.155123                     
-- owner: hdser                     
==========================
*/

WITH

mint_supply AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs
    WHERE
        contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1
),

burn_supply AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs
    WHERE
        contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1
),

net_supply AS (
    SELECT 
        day
        ,SUM(mint) AS mint
        ,SUM(burn) AS burn
        ,SUM(mint+burn) AS net
    FROM (
        SELECT 
            day
            ,value AS mint
            ,0 AS burn
        FROM
            mint_supply
        UNION ALL
        SELECT 
            day
            ,0 AS mint
            ,-value AS burn
        FROM
            burn_supply
    )
    GROUP BY 
        1
),

cumsum_supply AS (
    SELECT 
        day
        ,mint
        ,burn
        ,net
        ,SUM(net) OVER w AS supply
    FROM
        net_supply
    WINDOW w AS (
        ORDER BY 
            day
    )
),

last_day AS (
    SELECT
        day
        ,mint
        ,burn
        ,net
        ,supply
    FROM
        cumsum_supply
    ORDER BY
        day DESC
    LIMIT 1
)

SELECT 
    s.day
    ,s.mint
    ,s.burn
    ,s.net
    ,s.supply
    ,l.mint AS last_day_mint
    ,-l.burn AS last_day_burn
    ,l.net AS last_day_net
    ,l.supply AS last_day_supply
FROM
    cumsum_supply s
LEFT JOIN
    last_day l
    ON
    l.day = s.day
ORDER BY
    1 DESC