/*
======= Query Info =======                     
-- query_id: 3895267                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:57.039370                     
-- owner: hdser                     
==========================
*/

WITH

omen_gnosis_ai_agents_makers AS (
    SELECT
        address
        ,label
    FROM
        query_3584116
),

FixedProductMarketMakerCreation AS (
    SELECT 
        fixedProductMarketMaker
        ,creator
    FROM 
        omen_gnosis.FPMMDeterministicFactory_evt_FixedProductMarketMakerCreation
)


SELECT 
    t1.fixedProductMarketMaker 
    ,t2.label AS agent_label
    ,CASE
        WHEN t2.label = 'Replicator' THEN 'Gnosis'
        WHEN t2.label = 'Olas-market-creator' THEN 'Olas'
        ELSE 'Other'
    END AS label
FROM 
    FixedProductMarketMakerCreation t1
LEFT JOIN
    omen_gnosis_ai_agents_makers t2
    ON
    t2.address = t1.creator