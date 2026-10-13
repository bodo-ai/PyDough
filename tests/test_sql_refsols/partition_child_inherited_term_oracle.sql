SELECT
  REGION.r_regionkey AS rkey,
  MAX(REGION.r_name) AS region_name,
  COUNT(*) AS n_nations
FROM TPCH.REGION REGION
JOIN TPCH.NATION NATION
  ON NATION.n_regionkey = REGION.r_regionkey
GROUP BY
  REGION.r_regionkey
ORDER BY
  1 NULLS FIRST
