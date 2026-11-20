SELECT
  COUNT(*) AS n
FROM mongo.tpch.supplier AS supplier
JOIN cassandra.tpch.nation AS nation
  ON nation.n_nationkey = supplier.s_nationkey
JOIN mongo.tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AFRICA'
