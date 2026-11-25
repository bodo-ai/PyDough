SELECT
  c_custkey AS `key`,
  c_nationkey AS nk,
  ROW_NUMBER() OVER (
    PARTITION BY c_nationkey
    ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END DESC, c_acctbal DESC, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey
  ) AS rk
FROM tpch.CUSTOMER
WHERE
  c_acctbal > 9999
ORDER BY
  1
