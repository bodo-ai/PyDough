SELECT
  REGION.r_name AS region_name,
  COLLECT(NATION.n_name) AS nation_names
FROM TPCH.REGION REGION
JOIN TPCH.NATION NATION
  ON NATION.n_regionkey = REGION.r_regionkey
GROUP BY
  NATION.n_regionkey
ORDER BY
  1 NULLS FIRST
