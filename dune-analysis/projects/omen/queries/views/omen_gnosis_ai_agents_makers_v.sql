/*
======= Query Info =======                 
-- query_id: 3584116                 
-- description: ""                 
-- tags: []                 
-- parameters: []                 
-- last update: 2024-07-25 17:22:43.442019                 
-- owner: hdser                 
==========================
*/

WITH address_elements AS (
  SELECT
        t.address
        ,t.addr_order
    FROM
        UNNEST(ARRAY[
            0x89c5cc945dd550bcffb72fe42bff002429f46fec,
            0x993DFcE14768e4dE4c366654bE57C21D9ba54748
        ])  WITH ORDINALITY AS t(address, addr_order)
),
label_elements AS (
  SELECT
        t.label
        ,t.lbl_order
    FROM
        UNNEST(ARRAY[
            'Olas-market-creator',
            'Replicator'
        ])  WITH ORDINALITY AS t(label, lbl_order)
)


SELECT
  addr.address,
  lbl.label
FROM
  address_elements AS addr
JOIN
  label_elements AS lbl 
  ON addr.addr_order = lbl.lbl_order
      