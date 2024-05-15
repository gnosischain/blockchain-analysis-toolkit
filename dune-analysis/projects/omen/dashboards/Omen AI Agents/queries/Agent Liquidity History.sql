-- query_id: 3584370

WITH
omen_gnosis_liquidity AS (
    SELECT * FROM omen_gnosis.liquidity
),

omen_gnosis_markets AS (
    SELECT * FROM query_3668567
),

ai_agents_makers AS (
    SELECT * FROM query_3584116
),

ConditionalTokens_evt_PositionsMerge AS (
    SELECT 
        t1.evt_block_time AS block_time
        ,get_href(get_chain_explorer_tx_hash('gnosis', t1.evt_tx_hash), 'merge') AS action
        ,NULL AS outcome
        ,t1.amount
        , NULL AS outcomeTokens_amount
        ,t2.question
        ,t2.fixedproductmarketmaker
    FROM omen_gnosis.ConditionalTokens_evt_PositionsMerge t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.conditionId = t1.conditionid
    INNER JOIN
        ai_agents_makers AS t3
        ON t3.address = t1.evt_tx_from
    WHERE
    t3.label = '{{agent_maker}}'
),

ConditionalTokens_evt_PositionSplit AS (
    SELECT 
        t1.evt_block_time AS block_time
        ,get_href(get_chain_explorer_tx_hash('gnosis', t1.evt_tx_hash), 'split') AS action
        ,NULL AS outcome
        ,t1.amount
        , NULL AS outcomeTokens_amount
        ,t2.question
        ,t2.fixedproductmarketmaker
    FROM omen_gnosis.ConditionalTokens_evt_PositionSplit t1
    INNER JOIN
        omen_gnosis_markets t2
        ON t2.conditionId = t1.conditionid
    INNER JOIN
        ai_agents_makers AS t3
        ON t3.address = t1.evt_tx_from OR t3.address = t1.evt_tx_to
    WHERE
    t3.label = '{{agent_maker}}'
),

add_remove AS (
SELECT 
    t1.block_time
    ,get_href(get_chain_explorer_tx_hash('gnosis', t1.tx_hash), t1.action) AS action
    ,t1.outcomeIndex AS outcome
    ,NULL AS amount
    ,t1.outcomeTokens_amount
    ,t3.question
    ,t3.fixedproductmarketmaker
FROM
    omen_gnosis_liquidity t1
INNER JOIN
    ai_agents_makers AS t2
    ON t2.address = t1.tx_from OR t2.address = t1.tx_to
INNER JOIN
    omen_gnosis_markets t3
    ON t3.fixedproductmarketmaker = t1.fixedproductmarketmaker
WHERE
    t2.label = '{{agent_maker}}'
)

SELECT * FROM add_remove
UNION ALL 
SELECT * FROM ConditionalTokens_evt_PositionsMerge
UNION ALL 
SELECT * FROM ConditionalTokens_evt_PositionSplit
ORDER BY
    1