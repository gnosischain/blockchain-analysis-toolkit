/*
======= Query Info =======                     
-- query_id: 3898037                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:50.721123                     
-- owner: hdser                     
==========================
*/


SELECT
    IF(rn<=10, CAST(evt_tx_from AS VARCHAR), 'Others') AS address
    ,SUM(cnt) AS cnt
FROM (
    SELECT
        evt_tx_from
        ,cnt
        ,ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
    FROM (
        SELECT 
            --withdrawal_credentials
            evt_tx_from
            ,COUNT(*) AS cnt
        FROM gnosis_chain_gnosis.SBCDepositContract_evt_DepositEvent
        GROUP BY
            1
    )
)
GROUP BY 1