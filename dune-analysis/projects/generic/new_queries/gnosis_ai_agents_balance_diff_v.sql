/*
======= Query Info =======                     
-- query_id: 3870527                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:53.528717                     
-- owner: hdser                     
==========================
*/

WITH

omen_gnosis_ai_agents AS (
    SELECT * FROM query_3582994 --omen_gnosis_ai_agents_traders
    UNION ALL
    SELECT * FROM query_3584116 --omen_gnosis_ai_agents_makers
),

balance_diff AS (
    SELECT
        t1.block_time
        ,t1.evt_index
        ,t1.tx_hash
        ,t1."from" AS address
        ,t2.label
        ,t1.token_standard
        ,t1.contract_address AS token_address
        ,t1.symbol
        ,-CAST(t1.amount_raw AS INT256) AS amount_raw
        ,-t1.amount AS amount
        ,IF(t1.symbol = 'xDAI', -t1.amount, -t1.amount_usd) AS amount_usd
        FROM
           -- query_3935887 t1 --tokens_gnosis_transfers_bare_v ASSUMPTION: not validator, no suicide
            test_schema.git_dunesql_8b40f15_tokens_gnosis_transfers t1
           --tokens_gnosis.transfers t1
        INNER JOIN
            omen_gnosis_ai_agents t2
            ON 
            t2.address = t1."from"
        WHERE
            t1.block_time >= DATE '2024-01-01'
        
    UNION ALL
    
    SELECT
        t1.block_time
        ,t1.evt_index
        ,t1.tx_hash
        ,t1.to AS address
        ,t2.label
        ,t1.token_standard
        ,t1.contract_address AS token_address
        ,t1.symbol
        ,CAST(t1.amount_raw AS INT256) AS amount_raw
        ,t1.amount
        ,IF(t1.symbol = 'xDAI', t1.amount, t1.amount_usd) AS amount_usd
        FROM
            --query_3935887 t1 --tokens_gnosis_transfers_bare_v ASSUMPTION: not validator, no suicide
           test_schema.git_dunesql_8b40f15_tokens_gnosis_transfers t1
           -- tokens_gnosis.transfers t1
        INNER JOIN
            omen_gnosis_ai_agents t2
            ON 
            t2.address = t1.to
        WHERE
            t1.block_time >= DATE '2024-01-01'
)

SELECT * FROM balance_diff