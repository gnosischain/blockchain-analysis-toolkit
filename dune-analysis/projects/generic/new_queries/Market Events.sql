/*
======= Query Info =======                     
-- query_id: 3702665                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=fixedproductmarketmaker, value=0x3d2ab113682e5b66b94b1c3043f5885471e72036, type=enum)]                     
-- last update: 2024-07-25 17:22:54.947540                     
-- owner: hdser                     
==========================
*/

WITH

omen_gnosis_trades AS (
    SELECT * FROM omen_gnosis.trades
     WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

omen_gnosis_liquidity AS (
    SELECT * FROM omen_gnosis.liquidity
     WHERE  fixedproductmarketmaker = CAST({{fixedproductmarketmaker}} AS varbinary)
),

gnosis_trades_liquidity AS (
      SELECT
        block_time
        ,get_href(get_chain_explorer_tx_hash('gnosis', tx_hash), action) AS action
        ,evt_index
        ,ARRAY[outcomeindex] AS outcomeindex
      FROM omen_gnosis_trades
      UNION ALL
      SELECT
        block_time
        ,get_href(get_chain_explorer_tx_hash('gnosis', tx_hash), action) AS action
        ,evt_index
        ,outcomeindex
      FROM omen_gnosis_liquidity
)

SELECT 
    block_time AS "Block time"
    ,action AS "Action"
    ,outcomeindex AS "Outcome"
FROM gnosis_trades_liquidity
ORDER BY block_time