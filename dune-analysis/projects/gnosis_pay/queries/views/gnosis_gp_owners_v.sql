/*
======= Query Info =======                     
-- query_id: 3919956                     
-- description: ""                     
-- tags: []                     
-- parameters: [Parameter(name=timestamp, value=2024-07-14 18:00:00, type=datetime)]                     
-- last update: 2024-07-25 17:22:48.920768                     
-- owner: hdser                     
==========================
*/

SELECT DISTINCT
    owner AS address
FROM
    query_3707804
WHERE 
    creation_time <= TIMESTAMP '{{timestamp}}'