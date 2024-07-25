/*
======= Query Info =======                     
-- query_id: 3516044                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:49.949568                     
-- owner: hdser                     
==========================
*/

WITH

inflow AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day
        ,SUM(varbinary_to_uint256(varbinary_ltrim(data))/POWER(10,18)) AS value
    FROM
        gnosis.logs
    WHERE
        contract_address = 0x481c034c6d9441db23Ea48De68BCAe812C5d39bA -- block reward contract
        AND
        topic0 = 0x3c798bbcf33115b42c728b8504cff11dd58736e9fa789f1cda2738db7d696b2a --AddedReceiver 
    GROUP BY
        1
),

outflow AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day
        ,SUM(varbinary_to_uint256(varbinary_ltrim(varbinary_substring(data,33,64)))/POWER(10,18)) AS value
    FROM
        gnosis.logs
    WHERE
        contract_address = 0x7301cfa0e1756b71869e93d4e4dca5c7d0eb0aa6 -- Gnosis: xDai Bridge 2
        AND
        topic0 = 0x127650bcfb0ba017401abe4931453a405140a8fd36fece67bae2db174d3fdd63 --UserRequestForSignature 
    GROUP BY 
        1
),

calendar AS (
    SELECT
        DATE_TRUNC('day',time) AS day
    FROM
        gnosis.blocks
    WHERE
        number >= 9053325 --  Gnosis: xDai Bridge 2 contract creation block
    GROUP BY 
        1
)


SELECT 
    c.day
    ,COALESCE(i.value,0) AS inflow
    ,COALESCE(-o.value,0) AS outflow
    ,COALESCE(i.value,0) - COALESCE(o.value,0) AS net_value
    ,SUM(COALESCE(i.value,0) - COALESCE(o.value,0)) OVER w AS total
FROM
    calendar c
LEFT JOIN
    inflow i
    ON
    i.day = c.day
LEFT JOIN
    outflow o
    ON
    o.day = c.day
WINDOW w AS (
    ORDER BY c.day
)