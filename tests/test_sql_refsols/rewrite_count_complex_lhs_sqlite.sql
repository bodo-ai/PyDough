SELECT
  COUNT(*) AS n
FROM tpch.customer AS customer
JOIN tpch.nation AS nation
  ON customer.c_nationkey = nation.n_nationkey AND nation.n_name = 'CHINA'
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
