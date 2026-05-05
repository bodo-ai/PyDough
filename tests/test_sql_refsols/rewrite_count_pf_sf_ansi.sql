SELECT
  COUNT(*) AS n
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
JOIN tpch.customer AS customer
  ON customer.c_mktsegment = 'BUILDING' AND customer.c_nationkey = nation.n_nationkey
