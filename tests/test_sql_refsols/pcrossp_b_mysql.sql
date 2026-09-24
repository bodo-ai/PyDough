WITH _s0 AS (
  SELECT
    c_mktsegment,
    COUNT(*) AS n_rows
  FROM tpch.CUSTOMER
  GROUP BY
    1
), _s1 AS (
  SELECT
    p_mfgr,
    COUNT(*) AS n_rows
  FROM tpch.PART
  GROUP BY
    1
)
SELECT
  _s1.p_mfgr AS manufacturer,
  _s1.n_rows AS n_parts,
  _s0.c_mktsegment AS industry,
  _s0.n_rows AS n_custs
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
