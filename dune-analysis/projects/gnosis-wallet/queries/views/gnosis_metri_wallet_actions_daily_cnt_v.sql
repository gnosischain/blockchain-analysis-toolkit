/*
======= Query Info =======                     
-- query_id: 3821527                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:56.632559                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_metri_trust AS (
    SELECT
        CAST(evt_block_time AS DATE) AS block_date
        ,'CRC Trust' AS action
        ,method
        ,COUNT(DISTINCT evt_tx_hash) AS cnts
    FROM 
        query_3822102
    GROUP BY
        1, 2, 3
),

gnosis_metri_crc_mints AS (
    SELECT
        CAST(evt_block_time AS DATE) AS block_date
        ,'CRC Mint' AS action
        ,method
        ,COUNT(DISTINCT evt_tx_hash) AS cnts
    FROM 
        query_3820993
    GROUP BY
        1, 2, 3
),

gnosis_metri_crc_transfers AS (
    SELECT 
         CAST(evt_block_time AS DATE) AS block_date
        ,'CRC Transfer' AS action
        ,method
        ,COUNT(DISTINCT evt_tx_hash) AS cnts
    FROM 
        query_3821308
    GROUP BY
        1, 2, 3
),

gnosis_metri_monerium_transfers AS (
    SELECT
         CAST(evt_block_time AS DATE) AS block_date
        ,CASE
            WHEN token_address = 0xcB444e90D8198415266c6a2724b7900fb12FC56E THEN 'EURe Transfer'
            WHEN token_address = 0x5Cb9073902F2035222B9749F8fB0c9BFe5527108 THEN 'GBPe Transfer'
        END AS action
        ,method
        ,COUNT(DISTINCT evt_tx_hash) AS cnts
    FROM 
        query_3792681
    GROUP BY 
        1, 2, 3
)

SELECT * FROM gnosis_metri_trust
UNION ALL
SELECT * FROM gnosis_metri_crc_mints
UNION ALL 
SELECT * FROM gnosis_metri_crc_transfers
UNION ALL 
SELECT * FROM gnosis_metri_monerium_transfers
