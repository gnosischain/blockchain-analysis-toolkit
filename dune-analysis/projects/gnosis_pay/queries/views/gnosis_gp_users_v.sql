-- query_id: 3707804

WITH

gosis_pay_wallet AS (
    SELECT DISTINCT
        pay_wallet
    FROM (
        SELECT DISTINCT 
           "from" AS pay_wallet
        FROM 
            monerium_eure_gnosis.EURe_evt_Transfer
        WHERE
          "to" = 0x4822521E6135CD2599199c83Ea35179229A172EE -- Gnosis Pay aggregator
    
        UNION ALL
    
        SELECT DISTINCT 
           "from" AS pay_wallet
        FROM 
            monerium_gbpe_gnosis.GBP_evt_Transfer
        WHERE
          "to" = 0x4822521E6135CD2599199c83Ea35179229A172EE -- Gnosis Pay aggregator
    )
),

gnosis_SafeSetup AS (
    SELECT * FROM query_3629703
)

SELECT 
    t3.evt_block_time AS creation_time
    ,t1.pay_wallet 
    ,t2.owner
    ,t3.owners || t1.pay_wallet AS wallet_entity
    ,COUNT(*) OVER (ORDER BY t3.evt_block_time) AS entity_id
FROM gosis_pay_wallet t1
INNER JOIN
    gnosis_SafeSetup t3
    ON
    t3.contract_address = t1.pay_wallet
CROSS JOIN
    UNNEST(t3.owners) t2(owner)