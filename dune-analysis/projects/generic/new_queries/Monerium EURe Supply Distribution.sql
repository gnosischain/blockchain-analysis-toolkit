/*
======= Query Info =======                     
-- query_id: 3526141                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:57.684490                     
-- owner: hdser                     
==========================
*/

WITH

token_supply AS (
    SELECT
      SUM(CASE
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN varbinary_to_uint256(varbinary_ltrim(data))
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -varbinary_to_uint256(varbinary_ltrim(data))
        ELSE 0
        END )/ POWER(10,18) AS value
    FROM
        gnosis.logs
     WHERE
        contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        (
            topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000
            OR
            topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000
        )
),

in_supply AS (
    SELECT
      varbinary_substring(topic2,13,20) AS address
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))) AS value
    FROM
        gnosis.logs l
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        topic2 != 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1
),

out_supply AS (
    SELECT
      varbinary_substring(topic1,13,20) AS address
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))) AS value
    FROM
        gnosis.logs l
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        topic1 != 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1
),

net_supply AS (
    SELECT 
        address
        ,SUM(value)/POWER(10,18) AS supply
    FROM (
        SELECT  address, value FROM in_supply
        UNION ALL
        SELECT  address, -value AS value FROM out_supply
    )
    GROUP BY
        1
)


SELECT 
    address
    ,supply/(SELECT value FROM token_supply) AS supply_frac
FROM net_supply
WHERE supply/(SELECT value FROM token_supply) >= 0.01