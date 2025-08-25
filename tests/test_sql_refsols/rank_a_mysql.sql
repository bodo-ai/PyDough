SELECT
  c_custkey AS id,
  ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END DESC, c_acctbal DESC) AS rk
FROM tpch.customer
