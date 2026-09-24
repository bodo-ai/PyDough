WITH "_S0" AS (
  SELECT
    c_mktsegment AS C_MKTSEGMENT,
    COUNT(*) AS N_ROWS
  FROM TPCH.CUSTOMER
  GROUP BY
    c_mktsegment
), "_S1" AS (
  SELECT
    p_mfgr AS P_MFGR,
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  GROUP BY
    p_mfgr
)
SELECT
  "_S1".P_MFGR AS manufacturer,
  "_S1".N_ROWS AS n_parts,
  "_S0".C_MKTSEGMENT AS industry,
  "_S0".N_ROWS AS n_custs
FROM "_S0" "_S0"
CROSS JOIN "_S1" "_S1"
