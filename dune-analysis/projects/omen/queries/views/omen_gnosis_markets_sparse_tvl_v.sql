-- query_id: 3668377

WITH

omen_gnosis_trades_liquidity AS (
    SELECT fixedproductmarketmaker, tx_hash, action, NULL AS funder FROM omen_gnosis.trades
    UNION ALL
    SELECT fixedproductmarketmaker, tx_hash, action, funder FROM omen_gnosis.liquidity
),


omen_gnosis_markets AS (
    SELECT * FROM dune.hdser.query_3668567
    --dune.hdser.result_omen_gnosis_markets_mv
),

ConditionalTokens_evt_PositionsMerge AS (
    SELECT 
        t3.fixedproductmarketmaker
        ,t1.evt_block_time AS block_time
        ,'Merge' AS action
        ,t1.evt_tx_hash AS tx_hash
        ,t1.evt_index
        ,CAST(-t1.amount AS INT256) AS tvl_delta
    FROM omen_gnosis.ConditionalTokens_evt_PositionsMerge t1
    INNER JOIN
        omen_gnosis_markets t2
        ON 
        t2.conditionId = t1.conditionid
    INNER JOIN
        omen_gnosis_trades_liquidity t3
        ON
        t3.tx_hash = t1.evt_tx_hash
     --   AND
     --   (
     --       (t3.fixedproductmarketmaker = t1.stakeholder AND t3.action = 'Sell')
     --       OR
      --      (t3.funder = t1.stakeholder AND t3.action = 'Remove')
--        )
        
),

ConditionalTokens_evt_PositionSplit AS (
    SELECT 
        t3.fixedproductmarketmaker
        ,t1.evt_block_time AS block_time
        ,'Split' AS action
        ,t1.evt_tx_hash AS tx_hash
        ,t1.evt_index
        ,CAST(t1.amount AS INT256) AS tvl_delta
    FROM omen_gnosis.ConditionalTokens_evt_PositionSplit t1
    INNER JOIN
        omen_gnosis_markets t2
        ON 
        t2.conditionId = t1.conditionid
    INNER JOIN
        omen_gnosis_trades_liquidity t3
        ON
        t3.tx_hash = t1.evt_tx_hash
      --  AND
    --    t3.fixedproductmarketmaker = t1.stakeholder
     --   AND 
      --  (t3.action = 'Buy' OR t3.action = 'Add')
),

ConditionalTokens_evt_PayoutRedemption AS (
    SELECT 
        t2.fixedproductmarketmaker
        ,t1.evt_block_time AS block_time
        ,'Payout' AS action
        ,t1.evt_tx_hash AS tx_hash
        ,t1.evt_index
        ,CAST(-t1.payout AS INT256) AS tvl_delta
    FROM omen_gnosis.ConditionalTokens_evt_PayoutRedemption t1
    INNER JOIN
        omen_gnosis_markets t2
        ON 
        t2.conditionId = t1.conditionid
),

markets_tvl_delta AS (
    SELECT * FROM ConditionalTokens_evt_PositionsMerge
    UNION ALL
    SELECT * FROM ConditionalTokens_evt_PositionSplit
    UNION ALL
    SELECT * FROM ConditionalTokens_evt_PayoutRedemption
),

markets_tvl AS (
    SELECT
        t1.fixedproductmarketmaker
        ,t1.block_time
        ,t1.evt_index
        ,t1.tx_hash
        ,tvl/POWER(10,t3.decimals) AS tvl
        ,t1.tvl/POWER(10,t3.decimals) * t3.price AS tvl_usd
    FROM (
        SELECT 
            *
            ,SUM(tvl_delta) OVER (Partition BY fixedproductmarketmaker ORDER BY block_time, evt_index) AS tvl
        FROM markets_tvl_delta
    ) t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.fixedproductmarketmaker = t1.fixedproductmarketmaker
    LEFT JOIN
        prices.usd t3
        ON t3.contract_address = t2.collateraltoken
        AND t3.blockchain = 'gnosis'
        AND t3.minute = DATE_TRUNC('minute',t1.block_time)
)

SELECT * FROM markets_tvl