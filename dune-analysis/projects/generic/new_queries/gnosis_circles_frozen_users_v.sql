/*
======= Query Info =======                     
-- query_id: 3827079                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:54.813913                     
-- owner: hdser                     
==========================
*/

WITH

crc_user_info AS (
    SELECT 
        user
        ,token
    FROM circles_ubi_gnosis.Hub_evt_Signup
),

crc_user_mints AS (
    SELECT 
        wallet_address
        ,block_day
        ,COALESCE(block_day_lead, CURRENT_DATE) AS block_day_lead
        ,DATE_DIFF('day', block_day, COALESCE(block_day_lead, CURRENT_DATE)) AS days_between_mints
        ,CASE
            WHEN block_day_lead IS NULL THEN 'Waiting'
            ELSE 'Minted'
        END AS mint_status
    FROM (
        SELECT 
            wallet_address
            ,block_date AS block_day
            ,LEAD(block_date) OVER (PARTITION BY wallet_address ORDER BY block_date) AS block_day_lead
        FROM 
            query_3869915 --gnosis_circles_transfers_agg_day_v
        WHERE
            counterparty = 0x0000000000000000000000000000000000000000
    )
)


SELECT 
    *
FROM crc_user_mints
WHERE 
    mint_status = 'Waiting'
    AND
    days_between_mints >= 90
