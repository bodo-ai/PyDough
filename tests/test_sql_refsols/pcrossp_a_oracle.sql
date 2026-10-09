WITH "_s0" AS (
  SELECT
    P_MFGR,
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  GROUP BY
    P_MFGR
), "_s1" AS (
  SELECT
    C_MKTSEGMENT,
    COUNT(*) AS N_ROWS
  FROM TPCH.CUSTOMER
  GROUP BY
    C_MKTSEGMENT
)
SELECT
  "_s0".P_MFGR AS manufacturer,
  "_s0".N_ROWS AS n_parts,
  "_s1".C_MKTSEGMENT AS industry,
  "_s1".N_ROWS AS n_custs
FROM "_s0" "_s0"
CROSS JOIN "_s1" "_s1"
