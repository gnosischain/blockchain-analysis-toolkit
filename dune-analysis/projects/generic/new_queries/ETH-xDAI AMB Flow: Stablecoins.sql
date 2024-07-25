/*
======= Query Info =======                     
-- query_id: 3518057                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.275652                     
-- owner: hdser                     
==========================
*/

WITH

stablecoins AS (
    SELECT
        t.token_address
    FROM
        UNNEST(ARRAY[
            0xdd96B45877d0E8361a4DDb732da741e97f3191Ff, --BUSD
            0x7300AaFC0Ef0d47Daeb850f8b6a1931b40aCab33, --mUSD
            0xaBEf652195F98A91E490f047A5006B71c85f058d, --crvUSD
            0x4C36d2919e407f0Cc2Ee3c993ccF8ac26d9CE64e, --sUSD
            0xFe7ed09C4956f7cdb54eC4ffCB9818Db2D7025b8, --USDP
            0xB714654e905eDad1CA1940b7790A8239ece5A9ff, --TUSD
            0x9ec9551d4A1a1593b0ee8124D98590CC71b3B09D, --hUSDC
            0x91f8490eC27cbB1b2FaEdd29c2eC23011d7355FB, --hUSDT
            0x1e37E5b504F7773460d6eB0e24D2e7C223B66EC7, --HUSD
            0x4ECaBa5870353805a9F068101A40E0f32ed605C6, --USDT
            0xDDAfbb505ad214D7b80b1f830fcCc89B60fb7A83 --USDC
            ]) t(token_address)
),

tokens AS (
    SELECT 
        t.contract_address AS token_address
        ,t.symbol
        ,t.decimals
    FROM 
        tokens.erc20 t
    INNER JOIN
        stablecoins st
        ON
        st.token_address = t.contract_address
    WHERE 
        blockchain = 'gnosis'
),

prices AS (
    SELECT
        p.contract_address AS token_address
        ,p.minute
        ,p.price
    FROM 
        prices.usd p
    WHERE
        p.blockchain = 'gnosis'
        AND 
        p.minute >= (SELECT time FROM gnosis.blocks WHERE number = 11300566)
       
),

inflows AS (
    SELECT
        DATE_TRUNC('day',  l.block_time) AS day
        ,varbinary_ltrim(l.topic1) AS token_address
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals)) AS value
    FROM
        gnosis.logs l
    LEFT JOIN
        tokens t
        ON
        t.token_address = varbinary_ltrim(l.topic1)
    WHERE
        l.contract_address = 0xf6A78083ca3e2a662D6dd1703c939c8aCE2e268d -- OmniBridge
        AND
        l.topic0 = 0x9afd47907e25028cdaca89d193518c302bbb128617d5a992c5abd45815526593 --TokensBridged 
    GROUP BY
        1, 2
),

outflows AS (
    SELECT
        DATE_TRUNC('day',  l.block_time) AS day
        ,varbinary_ltrim(l.topic1) AS token_address
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals)) AS value
    FROM
        gnosis.logs l
    LEFT JOIN
        tokens t
        ON
        t.token_address = varbinary_ltrim(l.topic1)
    WHERE
        l.contract_address = 0xf6A78083ca3e2a662D6dd1703c939c8aCE2e268d -- OmniBridge
        AND
        l.topic0 = 0x59a9a8027b9c87b961e254899821c9a276b5efc35d1f7409ea4f291470f1629a --TokensBridgingInitiated 
    GROUP BY
        1, 2
),

net_flow AS (
    SELECT
        day
        ,token_address
        ,SUM(inflow) AS inflow
        ,SUM(outflow) AS outflow
        ,SUM(inflow) + SUM(outflow) AS netflow
    FROM
        (
        SELECT 
            day
            ,token_address
            ,value AS inflow
            ,0 AS outflow
        FROM
            inflows
        UNION ALL
        SELECT 
            day
            ,token_address
            ,0 AS inflow
            ,-value AS outflow
        FROM
            outflows
    )
    GROUP BY
        1, 2
),


start_dates AS (
    SELECT
        token_address
        ,MIN(day) AS day
    FROM
        net_flow
    GROUP BY
        1
),

calendar AS (
    SELECT
        DATE_TRUNC('day',time) AS day
    FROM
        gnosis.blocks
    WHERE
        number >= 11300566 -- OmniBridge contract creation block
    GROUP BY 
        1
),

tokens_calendar AS (
    SELECT 
        c.day
        ,s.token_address
    FROM
        calendar c
    CROSS JOIN
        start_dates s
    WHERE
        c.day >= s.day
        AND 
        s.token_address IS NOT NULL
)


SELECT 
    tc.day
    ,t.symbol
    ,COALESCE(n.inflow,0) AS inflow
    ,COALESCE(n.outflow,0) AS outflow
    ,COALESCE(n.netflow,0) AS netflow
    ,SUM(COALESCE(n.netflow,0)) OVER w AS total_flow
FROM
    tokens_calendar tc
LEFT JOIN
    net_flow n
    ON
    n.day = tc.day
    and
    n.token_address = tc.token_address
INNER JOIN
    tokens t
    ON 
    t.token_address = tc.token_address
WINDOW w AS (
    PARTITION BY
        tc.token_address
    ORDER BY 
        tc.day
)

