-- query_id: 3731998

WITH

gnosis_gp_wallet_flows_balance AS (
    SELECT * FROM query_3713262
)

SELECT
    cnt AS distinct_tokens
    ,COUNT(*) AS cnt
FROM
    (
        SELECT 
           entity_id
           ,COUNT(DISTINCT token_address) AS cnt
        FROM gnosis_gp_wallet_flows_balance
        GROUP BY 1
    )
GROUP BY 1