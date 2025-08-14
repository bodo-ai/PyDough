SELECT
  nation.n_name AS name,
  region.r_name AS rname
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON SUBSTRING(nation.n_name, 1, 1) = SUBSTRING(region.r_name, 1, 1)
  AND nation.n_regionkey = region.r_regionkey
ORDER BY
  nation.n_name
