/*
======= Query Info =======                     
-- query_id: 3520735                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:51.477333                     
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

mintburn_supply AS (
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

calendar AS (
    SELECT
        DATE_TRUNC('day', time) AS day
    FROM
        gnosis.blocks
    WHERE
        time>= (SELECT MIN(day) FROM mintburn_supply)
    GROUP BY 1
),

net_supply AS (
    SELECT
        t2.day
        ,COALESCE(t1.mint,0) AS mint
        ,COALESCE(t1.burn,0) AS burn
        ,COALESCE(t1.net,0) AS net
    FROM
        mintburn_supply t1
    RIGHT JOIN
        calendar t2
        ON
        t2.day = t1.day
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


supply_into_contracts AS (
    SELECT
      DATE_TRUNC('day', l.block_time) AS day
      ,t.wallets
      ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,18)) AS value
    FROM
        gnosis.logs l
    RIGHT JOIN
        contracts_label t
        ON
        t.address != varbinary_ltrim(l.topic1)
        AND
        t.address = varbinary_ltrim(l.topic2)
     WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        l.topic1 != 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1, 2
),

supply_out_contracts AS (
    SELECT
      DATE_TRUNC('day', l.block_time) AS day
      ,t.wallets
      ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,18)) AS value
    FROM
        gnosis.logs l
    RIGHT JOIN
        contracts_label t
        ON
        t.address = varbinary_ltrim(l.topic1)
        AND
        t.address != varbinary_ltrim(l.topic2)
    WHERE
        l.contract_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E -- Monerium: EURe Token
        AND
        l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef --Transfer
        AND
        l.topic2 != 0x0000000000000000000000000000000000000000000000000000000000000000
    GROUP BY
        1, 2
),

net_supply_contracts AS (
    SELECT 
        day
        ,wallets
        ,SUM(inflow) AS inflow
        ,SUM(outflow) AS outflow
        ,SUM(inflow+outflow) AS net
    FROM (
        SELECT 
            day
            ,wallets
            ,value AS inflow
            ,0 AS outflow
        FROM
            supply_into_contracts
        UNION ALL
        SELECT 
            day
            ,wallets
            ,0 AS inflow
            ,-value AS outflow
        FROM
            supply_out_contracts
    )
    GROUP BY 
        1, 2
),

calendar2 AS (
    SELECT day, 'contract' AS wallets FROM calendar
    UNION ALL
    SELECT day, 'safe' AS wallets FROM calendar
),

net_supply_contracts_full AS (
    SELECT
        t2.day
        ,t2.wallets
        ,COALESCE(t1.inflow,0) AS inflow
        ,COALESCE(t1.outflow,0) AS outflow
        ,COALESCE(t1.net,0) AS net
    FROM
        net_supply_contracts t1
    RIGHT JOIN
        calendar2 t2
        ON
        t2.day = t1.day
        AND
        t2.wallets = t1.wallets
        
),

cumsum_supply_contracts  AS (
    SELECT 
        day
        ,wallets
        ,inflow
        ,outflow
        ,net
        ,SUM(net) OVER w AS supply
    FROM
        net_supply_contracts_full
    WINDOW w AS (
        PARTITION BY
            wallets
        ORDER BY 
            day
    )
),

cumsum_supply_2 AS (
    SELECT
         day
        ,SUM(supply) AS supply
    FROM
        cumsum_supply_contracts
    GROUP BY
        1
),

supply_eoa AS (
SELECT
    'EOA' AS wallet
    ,ts.day
    ,ts.supply - cs.supply AS supply
FROM
    cumsum_supply ts
LEFT JOIN
    cumsum_supply_2 cs
    ON
    cs.day = ts.day
)

SELECT * FROM supply_eoa


