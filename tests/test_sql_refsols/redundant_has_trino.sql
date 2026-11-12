SELECT
  COUNT(*) AS n
FROM postgres.tpch.customer AS customer
JOIN cassandra.tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey
JOIN mongo.tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
