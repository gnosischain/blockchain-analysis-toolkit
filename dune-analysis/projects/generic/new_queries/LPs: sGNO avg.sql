/*
======= Query Info =======                     
-- query_id: 3665043                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.125114                     
-- owner: hdser                     
==========================
*/

WITH 

inflow AS (
  SELECT
    evt_block_time
    ,evt_block_number
    ,evt_tx_hash
    ,to AS user
    ,CAST(value AS INT256) AS value
  FROM 
    erc20_gnosis.evt_transfer
  WHERE
    contract_address in (0xbdf4488dcf7165788d438b62b4c8a333879b7078,0x2686d5E477d1AaA58BF8cE598fA95d97985c7Fb1)
    AND 
    evt_block_time >= FROM_UNIXTIME(1644944444) AT TIME ZONE 'UTC'
    AND 
    evt_block_time < FROM_UNIXTIME(1644944444 + 31536000) AT TIME ZONE 'UTC'
), 

outflow AS (
  SELECT
    evt_block_time
    ,evt_block_number
    ,evt_tx_hash
    ,"from" AS user
    ,CAST(-value AS INT256) AS value
  FROM 
    erc20_gnosis.evt_transfer
  WHERE
    contract_address in (0xbdf4488dcf7165788d438b62b4c8a333879b7078,0x2686d5E477d1AaA58BF8cE598fA95d97985c7Fb1)
    AND 
    evt_block_time >= FROM_UNIXTIME(1644944444) AT TIME ZONE 'UTC'
    AND 
    evt_block_time < FROM_UNIXTIME(1644944444 + 31536000) AT TIME ZONE 'UTC'
), 


balance_diff AS (
  SELECT 
    evt_block_number
    ,user
    ,MIN(evt_block_time) AS evt_block_time
    ,SUM(value) AS delta
  FROM (
    SELECT * FROM inflow
    UNION ALL
    SELECT * FROM outflow
  )
  WHERE user != 0x0000000000000000000000000000000000000000
  GROUP BY 1, 2
),

lead_balance_diff AS (
SELECT 
    *
    ,COALESCE(
        LEAD(evt_block_time) OVER (PARTITION BY user ORDER BY evt_block_number)
        ,FROM_UNIXTIME(1644944444 + 31536000) AT TIME ZONE 'UTC') AS evt_block_time_lead
    ,LEAD(evt_block_number) OVER (PARTITION BY user ORDER BY evt_block_number) AS evt_block_number_lead
FROM balance_diff
),

balance AS (
SELECT 
    evt_block_time
    ,user
    ,SUM(delta) OVER (PARTITION BY USER ORDER BY evt_block_number) AS amount
    ,date_diff('second', evt_block_time, evt_block_time_lead) AS delta_seconds
FROM lead_balance_diff
),

final AS (
    SELECT 
        user 
        ,(CAST(SUM(amount * delta_seconds) AS DOUBLE)/31536000)/1e18 AS avg_balance
    FROM
        balance
    GROUP BY 1
)

SELECT * FROM final
WHERE avg_balance != 0
AND 
--Gnosis DAO, and curve
user NOT IN (0x458cd345b4c05e8df39d0a07220feb4ec19f5e6f,0xbdf4488dcf7165788d438b62b4c8a333879b7078,0x2686d5E477d1AaA58BF8cE598fA95d97985c7Fb1)
ORDER BY 2 DESC

