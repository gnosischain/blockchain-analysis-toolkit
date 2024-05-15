-- query_id: 3644125

SELECT
  label AS "Label",
  GET_HREF(
    GET_CHAIN_EXPLORER_ADDRESS('gnosis', TRY_CAST(address AS VARCHAR)),
    TRY_CAST(address AS VARCHAR)
  ) AS "Address"
FROM query_3582994