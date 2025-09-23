SELECT
  c_custkey AS `key`,
  ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END, c_acctbal, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS a,
  ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END, c_acctbal, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS b,
  RANK() OVER (ORDER BY CASE WHEN c_mktsegment COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, c_mktsegment COLLATE utf8mb4_bin) AS c,
  DENSE_RANK() OVER (ORDER BY CASE WHEN c_mktsegment COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, c_mktsegment COLLATE utf8mb4_bin) AS d,
  NTILE(100) OVER (ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END, c_acctbal, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS e,
  NTILE(12) OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END, c_acctbal, CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS f,
  LAG(c_custkey, 1) OVER (ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS g,
  LAG(c_custkey, 2, -1) OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS h,
  LEAD(c_custkey, 1) OVER (ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS i,
  LEAD(c_custkey, 6000) OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_custkey IS NULL THEN 1 ELSE 0 END, c_custkey) AS j,
  SUM(c_acctbal) OVER (PARTITION BY c_nationkey) AS k,
  SUM(c_acctbal) OVER (ORDER BY c_custkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l,
  ROUND(AVG(c_acctbal) OVER (), 2) AS m,
  ROUND(
    AVG(c_acctbal) OVER (PARTITION BY c_nationkey ORDER BY c_custkey ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    2
  ) AS n,
  COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) OVER () AS o,
  COUNT(*) OVER () AS p
FROM tpch.CUSTOMER
ORDER BY
  1
LIMIT 10
