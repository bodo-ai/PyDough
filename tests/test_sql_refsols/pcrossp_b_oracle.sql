WITH "_s0" AS (
  SELECT
    C_MKTSEGMENT,
    COUNT(*) AS N_ROWS
  FROM TPCH.CUSTOMER
  GROUP BY
    C_MKTSEGMENT
), "_s1" AS (
  SELECT
    P_MFGR,
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  GROUP BY
    P_MFGR
)
SELECT
  "_s1".P_MFGR AS manufacturer,
  "_s1".N_ROWS AS n_parts,
  "_s0".C_MKTSEGMENT AS industry,
  "_s0".N_ROWS AS n_custs
FROM "_s0" "_s0"
CROSS JOIN "_s1" "_s1"
