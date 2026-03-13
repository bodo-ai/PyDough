SELECT
  c_custkey AS id,
  ROW_NUMBER() OVER (ORDER BY c_acctbal DESC NULLS FIRST) AS rk
FROM tpch.customer
