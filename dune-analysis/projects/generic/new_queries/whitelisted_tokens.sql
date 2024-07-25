/*
======= Query Info =======                     
-- query_id: 3529185                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.974502                     
-- owner: hdser                     
==========================
*/

WITH

tokens_array AS (
    SELECT
        t.token_address
    FROM
        UNNEST(ARRAY[
            0xcB444e90D8198415266c6a2724b7900fb12FC56E, -- Monerium: EURe Token
            0x4b1E2c2762667331Bc91648052F646d1b0d35984, -- Angle Protocol: agEUR Token
            0x9fB1d52596c44603198fB0aee434fac3a679f702, --  Jarvis Synthetic Euro
            0x8e5bBbb09Ed1ebdE8674Cda39A0c169401db4252, -- WBTC
            0x9C58BAcC331c9aa871AFD802DB6379a98e80CEdb, -- GNO
            0xdd96B45877d0E8361a4DDb732da741e97f3191Ff, --BUSD
            0x177127622c4A00F3d409B75571e12cB3c8973d3c, -- COW
            0x6A023CCd1ff6F2045C3309768eAd9E68F978f6e1, -- WETH
            0x4ECaBa5870353805a9F068101A40E0f32ed605C6, -- USDT
            0xDDAfbb505ad214D7b80b1f830fcCc89B60fb7A83 -- USDC
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
        tokens_array st
        ON
        st.token_address = t.contract_address
    WHERE 
        blockchain = 'gnosis'
)

SELECT * FROM tokens
ORDER BY symbol