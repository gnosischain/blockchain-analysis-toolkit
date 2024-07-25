/*
======= Query Info =======                     
-- query_id: 3935887                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.021285                     
-- owner: hdser                     
==========================
*/

WITH base_transfers as (
    SELECT
        *
    FROM
        query_3935643 --tokens_gnosis_base_transfers_v
    
), 
prices AS (
    SELECT
        day
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM
        prices.usd_daily
    
    WHERE
        day >= TIMESTAMP '2018-10-09'
)

SELECT
     t.blockchain
    , t.block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.evt_index
    , t.trace_address
    , t.token_standard
   -- , t.tx_from
--    , t.tx_to
--    , t.tx_index
    , t."from"
    , t.to
    , t.contract_address
    , CASE token_standard
    WHEN 'native' THEN evms_info.native_token_symbol
    WHEN 'erc20' THEN tokens_erc20.symbol
    ELSE NULL
END AS symbol
    , t.amount_raw
    , CASE token_standard
    WHEN 'native' THEN t.amount_raw / power(10, 18)
    WHEN 'erc20' THEN t.amount_raw / power(10, tokens_erc20.decimals)
    ELSE cast(t.amount_raw as double)
END AS amount
    , prices.price AS price_usd
    , CASE token_standard
    WHEN 'native' THEN (t.amount_raw / power(10, 18)) * prices.price
    WHEN 'erc20' THEN (t.amount_raw / power(10, tokens_erc20.decimals)) * prices.price
    ELSE NULL
END AS amount_usd
FROM
    base_transfers as t
INNER JOIN
    evms.info as evms_info
    ON evms_info.blockchain = t.blockchain
LEFT JOIN
    tokens.erc20 as tokens_erc20
    ON tokens_erc20.blockchain = t.blockchain
    AND tokens_erc20.contract_address = t.contract_address
LEFT JOIN
    prices
    ON date_trunc('day', t.block_time) = prices.day
    AND CASE
        WHEN t.token_standard = 'native'
            THEN
            prices.blockchain IS NULL
            AND prices.contract_address IS NULL
            AND evms_info.native_token_symbol = prices.symbol
        ELSE
            prices.blockchain = 'gnosis'
            AND t.contract_address = prices.contract_address
        END