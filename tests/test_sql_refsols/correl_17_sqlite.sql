SELECT
  CONCAT_WS('-', LOWER(region.r_name), LOWER(nation.n_name)) AS fullname
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
ORDER BY
  CONCAT_WS('-', LOWER(region.r_name), LOWER(nation.n_name))
