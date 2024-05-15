-- query_id: 3593795

WITH


omen_gnosis_markets_status AS (
    SELECT * FROM query_3601593
)

SELECT DISTINCT
    is_valid,
    status AS Status,
    SPLIT_PART(question, '␟', 1) AS Question,
    SPLIT_PART(question, '␟', 2) AS Answers,
    REGEXP_REPLACE(SPLIT_PART(question, '␟', 3) ,'[^A-Za-z]', '') AS Category,
    creation_time AS Creation,
    opening_time AS Opening,
    resolution_time AS Resolution,
    payoutNumerators AS "Payout Numerators",
    ABS(DATE_DIFF('second', NOW(), opening_time)) AS opening_distance_to_now,
    CASE
        WHEN status = 'Open' AND DATE_DIFF('second', NOW(), opening_time) < 86400 THEN 1
        ELSE 0
    END AS open_first
FROM omen_gnosis_markets_status
ORDER BY
    open_first DESC,
    opening_distance_to_now ASC,
    Status ASC