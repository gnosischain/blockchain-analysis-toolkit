/*
======= Query Info =======                 
-- query_id: 3582994                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:43.076936                 
-- owner: hdser                 
==========================
*/

WITH address_elements AS (
  SELECT
        t.address
        ,t.addr_order
    FROM
        UNNEST(ARRAY[
            0x2DD9f5678484C1F59F97eD334725858b938B4102,
            0x034c4ad84f7Ac6638bF19300d5BBe7D9b981e736,
            0xb611A9f02B318339049264c7a66ac3401281cc3c,
            0xE7aa88A1D044e5C987ECCe55aE8D2B562a41b72d,
            0xC918c15b87746E6351E5f0646DdCAaca11aF8568,
            0x1665A7432fDC6557c4a2385cC9F458302300583B,
            0x220E814643627f4Bc70814aF90e6a6b29433D685,
            0x89B0648e2DD0CB3E98f72873f1Fdec1e0F5eA72E,
            0xf4429dE7007E82FDc1e516767a0366e4d8D573d2,
            0x2fAe80e1418d9cE9806d61dc3368447247221aa6,
            0x7F429730D530E7514Fe5e40873B931096c403b53,
            0xc83037dd1c876E2b3c38257372B70d0FA3b41079,
            0xE593aCC8A255D3D0241C308EC1320BBdbC432981,
            0x220fFB0529ec8d5f84Dfbc5E4aBD9d1f0822f83a,
            0x993DFcE14768e4dE4c366654bE57C21D9ba54748,
            0x45d91b79e8DcAFf2b9E2761cA35a29368252D064,
            0x05E8BBdb89c84a14d05194bBbAE81CAF2340dB72
        ])  WITH ORDINALITY AS t(address, addr_order)
),

label_elements AS (
  SELECT
        t.label
        ,t.lbl_order
    FROM
        UNNEST(ARRAY[
            'PredictionProphetGPT3',
            'EvoOlasEmbeddingOA',
            'KnownOutcomeAgent',
            'PredictionProphetGPT4',
            'think-thoroughly',
            'mech_prediction-online',
            'mech_prediction-offline', 
            'mech_prediction-online-sme',
            'mech_prediction-offline-sme', 
            'mech_prediction-request-rag',
            'mech_prediction-request-reasoning', 
            'mech_prediction-url-cot', 
            'mech_prediction-with-research-bold',
            'microchain-agent',
            'CoinFlipAgent',
            'microchain-agent-updateable-prompt',
            'think-thoroughly-prophet'
        ])  WITH ORDINALITY AS t(label, lbl_order)
),

agents AS (
    SELECT
      addr.address,
      lbl.label
    FROM
      address_elements AS addr
    JOIN
      label_elements AS lbl 
      ON addr.addr_order = lbl.lbl_order
),

olas_agents AS (
    SELECT
        multisig AS address
        ,CONCAT('Olas','_',CAST(COUNT(*) OVER (ORDER BY evt_block_number, evt_index) AS VARCHAR)) AS label
    FROM (
        SELECT
            evt.multisig
            ,MIN(evt.evt_block_number) AS evt_block_number
            ,MIN(evt.evt_index) AS evt_index
        FROM 
            autonolas_gnosis.ServiceRegistryL2_evt_CreateMultisigWithAgents AS evt
        JOIN 
            autonolas_gnosis.ServiceRegistryL2_call_registerAgents AS call
        ON 
            evt.serviceId = call.serviceId
        WHERE 
            (call.agentIds[1] = 12 OR call.agentIds[1] = 14)
        GROUP BY 1
    )
),
  
final AS (
    SELECT address, label FROM agents
    UNION ALL 
    SELECT address, label FROM olas_agents
)

SELECT * FROM final
ORDER BY label