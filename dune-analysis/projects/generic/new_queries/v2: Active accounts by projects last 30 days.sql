/*
======= Query Info =======                     
-- query_id: 3625343                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.676292                     
-- owner: hdser                     
==========================
*/

WITH

projects AS (
    -- there are multiple names for the same address, keep one per address
    SELECT 
        address
        ,ARRAY_AGG(name ORDER BY name)[1] AS name 
    FROM query_2418768
    GROUP BY 1
),

transactions AS (
    SELECT * FROM query_2428358 
),

amb_bridge_txs AS (
    SELECT evt_tx_hash AS hash, 'AMB Bridge' AS project
    FROM (
        SELECT evt_block_time, sender, evt_tx_hash
        FROM bsc_xdai_amb_gnosis.HomeAMB_evt_AffirmationCompleted
        WHERE evt_block_time > now() - INTERVAL '30' day
    UNION ALL
        SELECT evt_block_time, sender, evt_tx_hash
        FROM eth_xdai_amb_gnosis.HomeAMB_evt_AffirmationCompleted
        WHERE evt_block_time > now() - INTERVAL '30' day
    ) AS amb_txs
    
),
inscription_txs AS (
    SELECT tx_hash AS hash, 'Inscriptions' AS project
    FROM inscription.mints m
    WHERE blockchain in ('gnosis')
    AND block_time > now() - INTERVAL '30' day
),

projects_txs AS (
    SELECT 
        t1."from" AS user
        ,t1.hash
        ,t1.block_time
        ,CASE
            WHEN t1."to" = t2.address THEN t2.name
            WHEN t2.address IS NULL THEN 
                CASE
                    WHEN t1.data IS NULL THEN 'EOA'
                    ELSE COALESCE(t3.project,t4.project,'Others')
                END  
        END AS project
    FROM transactions t1
    LEFT JOIN projects t2
        ON t2.address = t1."to"
    LEFT JOIN amb_bridge_txs t3
        ON t3.hash = t1.hash
    LEFT JOIN inscription_txs t4
        ON t4.hash = t1.hash
    WHERE 
        t1."to" IS NOT NULL -- remove contract creation
)



--SELECT SUM(cnt) FROM (
SELECT 
    project
    ,COUNT(DISTINCT user) AS cnt
FROM 
    projects_txs
GROUP BY 1
--)
