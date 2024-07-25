/*
======= Query Info =======                 
-- query_id: 3684914                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:42.942925                 
-- owner: hdser                 
==========================
*/

SELECT
     fixedProductMarketMaker
     ,collateralToken
    ,evt_block_time
    ,evt_index
    ,user
    ,ARRAY_AGG(value ORDER BY idx) AS outcomeTokens
FROM (
    SELECT
        fixedProductMarketMaker
        ,collateralToken
        ,evt_block_time
        ,evt_index
        ,user
        ,idx
        ,SUM(value) OVER (PARTITION BY fixedProductMarketMaker, user, idx ORDER BY evt_block_time, evt_index) AS value
    FROM
        query_3870892 --gnosis_omen_outcomeTokens_balance_diff_v
        ,UNNEST(delta_outcomeTokens) WITH ORDINALITY d(value,idx)
)
GROUP BY
    1,2,3,4,5