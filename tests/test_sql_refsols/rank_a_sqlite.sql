SELECT
  c_custkey AS id,
  ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) AS rk
FROM tpch.customer
