SELECT
  MAX(region.r_name) AS name,
  COUNT(*) AS n_prefix_nations
FROM tpch.region AS region
JOIN tpch.nation AS nation
  ON SUBSTRING(nation.n_name, 1, 1) = SUBSTRING(region.r_name, 1, 1)
  AND nation.n_regionkey = region.r_regionkey
GROUP BY
  nation.n_regionkey,
  SUBSTRING(nation.n_name, 1, 1)
