/*
======= Query Info =======                     
-- query_id: 3516066                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.461023                     
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
            0x8e5bBbb09Ed1ebdE8674Cda39A0c169401db4252, --WBTC
            0x44fA8E6f47987339850636F88629646662444217, --DAI
            0x9C58BAcC331c9aa871AFD802DB6379a98e80CEdb, --GNO
            0x6A023CCd1ff6F2045C3309768eAd9E68F978f6e1, --WETH
            0x177127622c4A00F3d409B75571e12cB3c8973d3c --COW
        )
),

prices AS (
    SELECT
        p.contract_address
        ,t.symbol
        ,p.minute
        ,p.price
    FROM 
        prices.usd p
    INNER JOIN
        tokens t
        ON 
        t.token_address = p.contract_address
    WHERE
        p.blockchain = 'gnosis'
        AND 
        p.minute >= (SELECT time FROM gnosis.blocks WHERE number = 11300566)
       
),

inflow AS (
    SELECT
        DATE_TRUNC('day',  l.block_time) AS day
        ,t.symbol
        ,varbinary_ltrim(l.topic1) AS token_address
       -- ,varbinary_ltrim(topic2) AS recipient
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals)) AS value
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals) * p.price) AS value_usd
    FROM
        gnosis.logs l
    LEFT JOIN
        tokens t
        ON
        t.token_address = varbinary_ltrim(l.topic1)
    LEFT JOIN
        prices p
        ON
        p.contract_address =  varbinary_ltrim(l.topic1)
        AND
        p.minute = DATE_TRUNC('minute',  l.block_time)
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
        --,varbinary_ltrim(topic2) AS sender
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals)) AS value
        ,SUM(varbinary_to_uint256(varbinary_ltrim(l.data))/POWER(10,t.decimals) * p.price) AS value_usd
    FROM
        gnosis.logs l
    LEFT JOIN
        prices p
        ON
        p.contract_address =  varbinary_ltrim(l.topic1)
        AND
        p.minute = DATE_TRUNC('minute',  l.block_time)
    LEFT JOIN
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
        --token_address
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
        AND 
        s.symbol IS NOT NULL
)


SELECT 
    tc.day
    ,tc.symbol
    ,COALESCE(i.value,0) AS inflow
    ,COALESCE(-o.value,0) AS outflow
    ,COALESCE(i.value,0) - COALESCE(o.value,0) AS net_value
    ,SUM(COALESCE(i.value,0) - COALESCE(o.value,0)) OVER w AS total
    
    ,COALESCE(i.value_usd,0) AS inflow_usd
    ,COALESCE(-o.value_usd,0) AS outflow_usd
    ,(COALESCE(i.value,0) - COALESCE(o.value,0))* p.price AS net_value_usd
    
    
    ,(SUM(COALESCE(i.value,0) - COALESCE(o.value,0)) OVER w) * p.price AS total_usd
    ,p.price
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
    o.symbol = i.symbol
LEFT JOIN
        prices p
        ON
        p.symbol =  tc.symbol
        AND
        p.minute = DATE_TRUNC('minute',  tc.day)
WINDOW w AS (
    PARTITION BY
        tc.symbol
    ORDER BY 
        tc.day
)
ORDER BY 1, 2
    