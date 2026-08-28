SELECT
  REGION.r_name COLLATE utf8mb4_bin AS region_name,
  JSON_ARRAYAGG(NATION.n_name) AS nation_names
FROM tpch.REGION AS REGION
JOIN tpch.NATION AS NATION
  ON NATION.n_regionkey = REGION.r_regionkey
GROUP BY
  NATION.n_regionkey
ORDER BY
  1
