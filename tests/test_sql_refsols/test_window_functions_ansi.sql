SELECT
  DENSE_RANK() OVER (ORDER BY c_acctbal DESC NULLS FIRST) AS rank_value,
  NTILE(10) OVER (ORDER BY c_acctbal NULLS LAST) AS precentile_value,
  LAG(c_acctbal, 2) OVER (ORDER BY c_acctbal NULLS LAST) AS two_prev_value,
  LEAD(c_acctbal, 2) OVER (ORDER BY c_acctbal NULLS LAST) AS two_next_value,
  SUM(c_acctbal) OVER (ORDER BY c_acctbal NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS relsum_value,
  c_acctbal / AVG(c_acctbal) OVER () AS relavg_value,
  c_acctbal / COUNT(CASE WHEN c_acctbal > 0.0 THEN c_acctbal ELSE NULL END) OVER () AS relcount_value,
  c_acctbal / COUNT(*) OVER () AS relsize_value
FROM tpch.customer
