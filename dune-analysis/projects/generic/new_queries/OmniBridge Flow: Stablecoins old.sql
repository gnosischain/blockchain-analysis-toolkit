/*
======= Query Info =======                     
-- query_id: 3516508                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.062813                     
-- owner: hdser                     
==========================
*/

WITH

tokens AS (
    SELECT 
        contract_address AS token_address
        ,symbol
        ,decimals
    FROM 
        tokens.erc20
    WHERE 
        blockchain = 'gnosis'
        AND
        contract_address IN (
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
        )
),

inflow AS (
    SELECT
        DATE_TRUNC('day',  l.block_time) AS day
        ,t.symbol
        ,varbinary_ltrim(l.topic1) AS token_address
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals)) AS value
    FROM
        gnosis.logs l
    INNER JOIN
        tokens t
        ON
        t.token_address = varbinary_ltrim(l.topic1)
    WHERE
        l.contract_address = 0xf6A78083ca3e2a662D6dd1703c939c8aCE2e268d -- OmniBridge
        AND
        l.topic0 = 0x9afd47907e25028cdaca89d193518c302bbb128617d5a992c5abd45815526593 --TokensBridged 
    GROUP BY
        1, 2, 3
),

outflow AS (
    SELECT
        DATE_TRUNC('day',  l.block_time) AS day
        ,t.symbol
        ,varbinary_ltrim(l.topic1) AS token_address
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals)) AS value
    FROM
        gnosis.logs l
    INNER JOIN
        tokens t
        ON
        t.token_address = varbinary_ltrim(l.topic1)
    WHERE
        l.contract_address = 0xf6A78083ca3e2a662D6dd1703c939c8aCE2e268d -- OmniBridge
        AND
        l.topic0 = 0x59a9a8027b9c87b961e254899821c9a276b5efc35d1f7409ea4f291470f1629a --TokensBridgingInitiated 
    GROUP BY
        1, 2, 3
),

start_dates AS (
    SELECT
        symbol
        ,MIN(day) AS day
    FROM
        inflow
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
        ,s.symbol
    FROM
        calendar c
    CROSS JOIN
        start_dates s
    WHERE
        c.day >= s.day
)


SELECT 
    tc.day
    ,tc.symbol
    ,COALESCE(i.value,0) AS inflow
    ,COALESCE(-o.value,0) AS outflow
    ,COALESCE(i.value,0) - COALESCE(o.value,0) AS net_value
    ,SUM(COALESCE(i.value,0) - COALESCE(o.value,0)) OVER w AS total
FROM
    tokens_calendar tc
LEFT JOIN
    inflow i
    ON
    i.day = tc.day
    and
    i.symbol = tc.symbol
LEFT JOIN
    outflow o
    ON
    o.day = tc.day
    and
    o.symbol = tc.symbol
WINDOW w AS (
    PARTITION BY
        tc.symbol
    ORDER BY 
        tc.day
)

    