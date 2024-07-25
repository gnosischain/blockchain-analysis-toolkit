/*
======= Query Info =======                     
-- query_id: 3821724                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.320853                     
-- owner: hdser                     
==========================
*/

WITH

gnosis_metri_crc_mints AS (
    SELECT 
        CAST(evt_block_time AS DATE) AS block_date
        ,wallet_address
        ,method
    FROM 
        query_3820993
),

gnosis_metri_crc_transfers AS (
    SELECT 
         CAST(evt_block_time AS DATE) AS block_date
        ,wallet_address
        ,method
    FROM 
        query_3821308
),

gnosis_metri_monerium_transfers AS (
    SELECT 
         CAST(evt_block_time AS DATE) AS block_date
        ,wallet_address
        ,method
    FROM 
        query_3792681
),

gnosis_wallet_address_appearances AS (
    SELECT DISTINCT * FROM (
        SELECT * FROM gnosis_metri_crc_mints
        UNION ALL 
        SELECT * FROM gnosis_metri_crc_transfers
        UNION ALL 
        SELECT * FROM gnosis_metri_monerium_transfers
    )
)


SELECT
    wallet_address
    ,method
    ,block_date
    ,COALESCE(block_date_lead, CURRENT_DATE) AS block_date_lead
    ,DATE_DIFF('day', block_date + INTERVAL '1' DAY, COALESCE(block_date_lead, CURRENT_DATE)) AS inactive_days
    ,CASE
        WHEN block_date_lead IS NULL THEN 'inactive'
        ELSE 'active'
    END AS status
FROM (
    SELECT
        wallet_address
        ,method
        ,block_date 
        ,LEAD(block_date) OVER (PARTITION BY wallet_address ORDER BY block_date) AS block_date_lead
    FROM 
        gnosis_wallet_address_appearances
)
