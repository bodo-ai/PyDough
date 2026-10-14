SELECT
  REGION.r_regionkey AS rkey,
  MAX(REGION.r_name) AS region_name,
  COUNT(*) AS n_nations
FROM tpch.REGION AS REGION
JOIN tpch.NATION AS NATION
  ON NATION.n_regionkey = REGION.r_regionkey
GROUP BY
  1
ORDER BY
  1
