/*
======= Query Info =======                     
-- query_id: 3616754                     
-- description: ""                     
-- tags: []                     
-- parameters: []                     
-- last update: 2024-07-25 17:22:57.479215                     
-- owner: hdser                     
==========================
*/

WITH

Realitio_raw AS (
    SELECT
        CASE
            WHEN topic0 = 0xfe2dac156a3890636ce13f65f4fdf41dcaee11526e4a5374531572d92194796c THEN 'Realitio_v2_1_evt_LogNewQuestion'
            WHEN topic0 = 0xe47ca4ebbbc2990134d1168821f38c5e177f3d5ee564bffeadeaa351905e6221 THEN 'Realitio_v2_1_evt_LogNewAnswer'
            WHEN topic0 = 0x9c121aff33b50c1a53fef034ebec5f83da2d5a5187048f9c76c397ba27c1a1a6 THEN 'Realitio_v2_1_evt_LogClaim'
            WHEN topic0 = 0x18d760beffe3717270cd90d9d920ec1a48c194e9ad7bba23eb1c92d3eb974f97 THEN 'Realitio_v2_1_evt_LogFinalize'
            WHEN topic0 = 0xb87fb721c0a557bb8dff89a86796466931d82ba530a66a239263eb8735ade2e4 THEN 'Realitio_v2_1_evt_LogNewTemplate'
            WHEN topic0 = 0x75d7939999bc902187c4aed400872883e445145f1983539166f783fa040b4762 THEN 'Realitio_v2_1_evt_LogNotifyOfArbitrationRequest'
            WHEN topic0 = 0x4ce7033d118120e254016dccf195288400b28fc8936425acd5f17ce2df3ab708 THEN 'Realitio_v2_1_evt_LogWithdraw'
        END AS contract
        ,MIN(block_time) AS block_time
    FROM 
        gnosis.logs
    WHERE
        (
        topic0 = 0xfe2dac156a3890636ce13f65f4fdf41dcaee11526e4a5374531572d92194796c --omen_gnosis.Realitio_v2_1_evt_LogNewQuestion
        OR
        topic0 = 0xe47ca4ebbbc2990134d1168821f38c5e177f3d5ee564bffeadeaa351905e6221 --omen_gnosis.Realitio_v2_1_evt_LogNewAnswer
        OR 
        topic0 = 0x9c121aff33b50c1a53fef034ebec5f83da2d5a5187048f9c76c397ba27c1a1a6 --omen_gnosis.Realitio_v2_1_evt_LogClaim
        OR
        topic0 = 0x18d760beffe3717270cd90d9d920ec1a48c194e9ad7bba23eb1c92d3eb974f97 --omen_gnosis.Realitio_v2_1_evt_LogFinalize
        OR
        topic0 = 0xb87fb721c0a557bb8dff89a86796466931d82ba530a66a239263eb8735ade2e4 --omen_gnosis.Realitio_v2_1_evt_LogNewTemplate
        OR
        topic0 = 0x75d7939999bc902187c4aed400872883e445145f1983539166f783fa040b4762 --omen_gnosis.Realitio_v2_1_evt_LogNotifyOfArbitrationRequest
        OR 
        topic0 = 0x4ce7033d118120e254016dccf195288400b28fc8936425acd5f17ce2df3ab708 --omen_gnosis.Realitio_v2_1_evt_LogWithdraw
        )
    AND block_time >= TIMESTAMP '2018-12-01'
    AND block_time <= TIMESTAMP '2022-01-01'
    GROUP BY
        1
),

Realitio_decoded AS (
    SELECT
        'Realitio_v2_1_evt_LogNewQuestion' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogNewQuestion
    WHERE
        evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1
    
    UNION ALL
    
    SELECT
        'Realitio_v2_1_evt_LogNewAnswer' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogNewAnswer
    WHERE
        evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1
    
    UNION ALL
    
    SELECT
        'Realitio_v2_1_evt_LogClaim' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogClaim
    WHERE
        evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1
    
    UNION ALL
    
    SELECT
        'Realitio_v2_1_evt_LogFinalize' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogFinalize
    WHERE
        evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1

    UNION ALL
    
    SELECT
        'Realitio_v2_1_evt_LogNewTemplate' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogNewTemplate
    WHERE
        evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1
    
    UNION ALL
    
    SELECT
        'Realitio_v2_1_evt_LogNotifyOfArbitrationRequest' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogNotifyOfArbitrationRequest
    WHERE
        evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1
    
    UNION ALL
    
    SELECT
        'Realitio_v2_1_evt_LogWithdraw' AS contract
        ,MIN(evt_block_time) AS block_time
    FROM 
        omen_gnosis.Realitio_v2_1_evt_LogWithdraw
    WHERE
         evt_block_time <= TIMESTAMP '2022-01-01'
    GROUP BY 1
        
)

SELECT
    t1.contract
    ,t1.block_time AS block_time_logs
    ,t2.block_time AS block_time_decoded
FROM Realitio_raw t1
LEFT JOIN Realitio_decoded t2
ON t2.contract = t1.contract
    
