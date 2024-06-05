-- query_id: 3714169

WITH

wallet_flows_total AS (
    SELECT
        SUM(IF(action = 'Burn',value,0)) AS burn 
        ,SUM(IF(action = 'Mint',value,0)) AS mint
        ,SUM(IF(action = 'Inflow',value,0)) AS inflow
        ,SUM(IF(action = 'Outflow',value,0)) AS outflow
    FROM query_3713262 --gnosis_gp_wallet_flows_balance
    WHERE 
        token_address = CAST({{token_address}} AS varbinary)
        AND
        action != 'Balance'
),

final AS (
    SELECT 
        t1.mint/POWER(10,COALESCE(t2.decimals,18)) AS mint
        ,t1.burn/POWER(10,COALESCE(t2.decimals,18)) AS burn
        ,t1.inflow/POWER(10,COALESCE(t2.decimals,18)) AS inflow
        ,t1.outflow/POWER(10,COALESCE(t2.decimals,18)) AS outflow
        ,(t1.mint + t1.burn + t1.inflow + t1.outflow)/POWER(10,COALESCE(t2.decimals,18)) AS balance
    FROM wallet_flows_total t1
    LEFT JOIN
        tokens.erc20 t2
        ON 
        t2.contract_address = CAST({{token_address}} AS varbinary)
)

    
SELECT * FROM final
