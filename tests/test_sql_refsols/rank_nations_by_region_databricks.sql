SELECT
  nation.n_name AS name,
  RANK() OVER (ORDER BY region.r_name NULLS LAST) AS rank
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey
