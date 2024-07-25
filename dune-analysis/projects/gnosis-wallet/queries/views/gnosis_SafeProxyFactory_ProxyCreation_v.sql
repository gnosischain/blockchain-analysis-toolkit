/*
======= Query Info =======                 
-- query_id: 3630076                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:46.949953                 
-- owner: hdser                 
==========================
*/

WITH

ProxyFactory_v1_0_0_evt_ProxyCreation AS (
    SELECT 
        contract_address
        ,evt_tx_hash
        ,evt_tx_from
        ,evt_tx_to
        ,evt_index
        ,evt_block_time
        ,evt_block_number
        ,evt_block_date
        ,proxy
        ,NULL AS singleton
        ,'v1.0.0' AS version
    FROM
        gnosis_safe_gnosis.ProxyFactory_v1_0_0_evt_ProxyCreation
),

ProxyFactory_v1_1_1_evt_ProxyCreation AS (
    SELECT 
        contract_address
        ,evt_tx_hash
        ,evt_tx_from
        ,evt_tx_to
        ,evt_index
        ,evt_block_time
        ,evt_block_number
        ,evt_block_date
        ,proxy
        ,NULL AS singleton
        ,'v1.1.1' AS version
    FROM
        gnosis_safe_gnosis.ProxyFactory_v1_1_1_evt_ProxyCreation
),

GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation AS (
    SELECT 
        contract_address
        ,evt_tx_hash
        ,evt_tx_from
        ,evt_tx_to
        ,evt_index
        ,evt_block_time
        ,evt_block_number
        ,evt_block_date
        ,proxy
        ,singleton
        ,'v1.3.0' AS version
    FROM
        gnosis_safe_gnosis.GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation
),

SafeProxyFactory_v_1_4_1_evt_ProxyCreation AS (
    SELECT 
        contract_address
        ,evt_tx_hash
        ,evt_tx_from
        ,evt_tx_to
        ,evt_index
        ,evt_block_time
        ,evt_block_number
        ,evt_block_date
        ,proxy
        ,singleton
        ,'v1.4.1' AS version
    FROM
        gnosis_chain_gnosis.SafeProxyFactory_v_1_4_1_evt_ProxyCreation
),


final AS (
    SELECT * FROM ProxyFactory_v1_0_0_evt_ProxyCreation
    UNION ALL
    SELECT * FROM ProxyFactory_v1_1_1_evt_ProxyCreation
    UNION ALL
    SELECT * FROM GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation
    UNION ALL
    SELECT * FROM SafeProxyFactory_v_1_4_1_evt_ProxyCreation
)

SELECT * FROM final