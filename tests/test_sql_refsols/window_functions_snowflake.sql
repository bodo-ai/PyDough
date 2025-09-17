SELECT
  DENSE_RANK() OVER (ORDER BY customer.c_acctbal DESC) AS rank_value,
  NTILE(10) OVER (ORDER BY customer.c_acctbal) AS precentile_value,
  LAG(customer.c_acctbal, 2, 0.0) OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal) AS two_prev_value,
  LEAD(customer.c_acctbal, 2) OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal) AS two_next_value,
  SUM(customer.c_acctbal) OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS relsum_value,
  SUM(customer.c_acctbal) OVER (ORDER BY customer.c_acctbal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS relsum_value2,
  customer.c_acctbal / AVG(customer.c_acctbal) OVER (ORDER BY customer.c_acctbal ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS relavg_value,
  customer.c_acctbal / COUNT(CASE WHEN customer.c_acctbal > 0.0 THEN customer.c_acctbal ELSE NULL END) OVER () AS relcount_value,
  customer.c_acctbal / COUNT(*) OVER () AS relsize_value
FROM tpch.nation AS nation
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
