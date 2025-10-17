SELECT
  ANY_VALUE(nation.n_name) AS nation_name,
  COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS n_black_acctbal,
  MEDIAN(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) AS median_red_acctbal,
  MEDIAN(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) AS median_black_acctbal,
  MEDIAN(customer.c_acctbal) AS median_overall_acctbal
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
GROUP BY
  customer.c_nationkey
ORDER BY
  1 NULLS FIRST
