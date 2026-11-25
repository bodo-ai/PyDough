SELECT
  c_custkey AS key,
  c_nationkey AS nk,
  ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC, c_custkey) AS rk
FROM tpch.customer
WHERE
  c_acctbal > 9999
ORDER BY
  1 NULLS FIRST
