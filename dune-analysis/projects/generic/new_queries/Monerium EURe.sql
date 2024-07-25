/*
======= Query Info =======                     
-- query_id: 3525051                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.164587                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_safe AS (
    SELECT 
    tx_hash
     ,CASE
        WHEN contract_address = 0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67
            THEN varbinary_substring(topic1,13,20)
        ELSE varbinary_substring(data,13,20)  
     END AS address
     ,CASE
        WHEN contract_address = 0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2 
            THEN varbinary_substring(data,45,20)
        WHEN contract_address = 0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67
            THEN varbinary_substring(data,13,20)
     END AS singleton
    FROM
        gnosis.logs
    WHERE
        (
            (
                contract_address = 0x12302fE9c02ff50939BaAaaf415fc226C078613C --ProxyFactory 1.0.0
                OR
                contract_address = 0x76E2cFc1F5Fa8F6a5b3fC4c8F4788F0116861F9B --ProxyFactory 1.1.1
            )
            AND
            topic0 = 0xa38789425dbeee0239e16ff2d2567e31720127fbc6430758c1a4efc6aef29f80 --ProxyCreation 
        )
        OR
        (
            (
                contract_address = 0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2 --ProxyFactory 1.3.0
                OR
                contract_address = 0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67 --ProxyFactory 1.4.1
            )
            AND
            topic0 = 0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235 --ProxyCreation 
        )
),

contracts_label AS (
    SELECT
        CASE
            WHEN s.address = t.address THEN 'safe'
            ELSE 'contract'
        END AS wallets
        ,t.address
    FROM
        gnosis.creation_traces t
    LEFT JOIN
        gnosis_safe s
        ON
        s.address = t.address
),

mintburn_supply AS (
    SELECT
      DATE_TRUNC('day', l.block_time) AS day
      ,CASE
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN 'mint'
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN 'burn'
        END AS action
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs l
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        (
            topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000
            OR
            topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000
        )
    GROUP BY
        1, 2
),

mint_contract_supply AS (
    SELECT
      DATE_TRUNC('day', l.block_time) AS day
      , CONCAT(t.wallets, '_mint') AS action
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs l
    RIGHT JOIN
        contracts_label t
        ON
        t.address = varbinary_substring(topic2,13,20)
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        varbinary_substring(topic1,13,20) NOT IN (SELECT address FROM contracts_label)
    GROUP BY
        1, 2
),

burn_contract_supply AS (
    SELECT
      DATE_TRUNC('day', l.block_time) AS day
      , CONCAT(t.wallets, '_burn') AS action
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs l
    RIGHT JOIN
        contracts_label t
        ON
        t.address = varbinary_substring(topic1,13,20)
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        varbinary_substring(topic2,13,20) NOT IN (SELECT address FROM contracts_label)
    GROUP BY
        1, 2
),

burn_mint_mix_supply AS (
    SELECT
      DATE_TRUNC('day', l.block_time) AS day
      , CONCAT(t1.wallets, '_burn') AS action1
      , CONCAT(t2.wallets, '_mint') AS action2
      ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs l
    RIGHT JOIN
        contracts_label t1
        ON
        t1.address = varbinary_substring(topic1,13,20)
    RIGHT JOIN
        contracts_label t2
        ON
        t2.address = varbinary_substring(topic2,13,20)
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        t1.wallets != t2.wallets
    GROUP BY
        1, 2, 3
),

mix_supply AS (
    SELECT day, action1 AS action, value FROM burn_mint_mix_supply
    UNION ALL
    SELECT day, action2 AS action, value FROM burn_mint_mix_supply
),

supply_actions AS (
    SELECT
        day
        ,action
        ,SUM(value) AS value
    FROM (
        SELECT * FROM mintburn_supply
        UNION ALL
        SELECT * FROM mint_contract_supply
        UNION ALL
        SELECT * FROM burn_contract_supply
        UNION ALL
        SELECT * FROM mix_supply
    )
    GROUP BY
        1, 2
),

calendar AS (
    SELECT
        DATE_TRUNC('day', time) AS day
    FROM
        gnosis.blocks
    WHERE
        time>= (SELECT MIN(day) FROM supply_actions)
    GROUP BY 1
),

calendar_action AS (
    SELECT day, 'mint' AS action FROM calendar
    UNION ALL
    SELECT day, 'burn' AS action FROM calendar
    UNION ALL
    SELECT day, 'safe_mint' AS action FROM calendar
    UNION ALL
    SELECT day, 'safe_burn' AS action FROM calendar
    UNION ALL
    SELECT day, 'contract_mint' AS action FROM calendar
    UNION ALL
    SELECT day, 'contract_burn' AS action FROM calendar
),

supply_actions_full AS (
    SELECT
        t2.day
        ,t1.action
        ,COALESCE(t1.value,0) AS value
    FROM
        supply_actions t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.day
       -- AND
    --    t2.action = t1.action
        
),

mint_burn_suply AS (
    SELECT 
        day
        ,SUM(CASE
            WHEN action = 'mint' THEN value
            ELSE 0
        END) AS mint 
        ,SUM(CASE
            WHEN action = 'burn' THEN -value
            ELSE 0
        END) AS burn
        ,SUM(CASE
            WHEN action = 'safe_mint' THEN value
            ELSE 0
        END) AS safe_mint 
        ,SUM(CASE
            WHEN action = 'safe_burn' THEN -value
            ELSE 0
        END) AS safe_burn
        ,SUM(CASE
            WHEN action = 'contract_mint' THEN value
            ELSE 0
        END) AS contract_mint 
        ,SUM(CASE
            WHEN action = 'contract_burn' THEN -value
            ELSE 0
        END) AS contract_burn
    FROM 
        supply_actions_full
    GROUP BY 
        1
),

net_suply AS (
    SELECT 
        day
        ,mint
        ,burn
        ,mint + burn AS net
        ,safe_mint
        ,safe_burn
        ,safe_mint + safe_burn AS safe_net
        ,contract_mint
        ,contract_burn
        ,contract_mint + contract_burn AS contract_net
    FROM 
        mint_burn_suply
)

SELECT 
    *
    ,sum(net) OVER w AS supply
    ,sum(safe_net) OVER w AS safe_supply
    ,sum(contract_net) OVER w AS contract_supply
    ,sum(net-safe_net-contract_net) OVER w AS eoa_supply
FROM
    net_suply
WINDOW w AS (
    ORDER BY
        day
)
ORDER BY 
    day DESC


