SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
