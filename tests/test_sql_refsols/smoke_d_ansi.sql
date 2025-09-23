SELECT
  c_custkey AS key,
  ROW_NUMBER() OVER (ORDER BY c_acctbal NULLS LAST, c_custkey NULLS LAST) AS a,
  ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal NULLS LAST, c_custkey NULLS LAST) AS b,
  RANK() OVER (ORDER BY c_mktsegment NULLS LAST) AS c,
  DENSE_RANK() OVER (ORDER BY c_mktsegment NULLS LAST) AS d,
  NTILE(100) OVER (ORDER BY c_acctbal NULLS LAST, c_custkey NULLS LAST) AS e,
  NTILE(12) OVER (PARTITION BY c_nationkey ORDER BY c_acctbal NULLS LAST, c_custkey NULLS LAST) AS f,
  LAG(c_custkey, 1) OVER (ORDER BY c_custkey NULLS LAST) AS g,
  LAG(c_custkey, 2, -1) OVER (PARTITION BY c_nationkey ORDER BY c_custkey NULLS LAST) AS h,
  LEAD(c_custkey, 1) OVER (ORDER BY c_custkey NULLS LAST) AS i,
  LEAD(c_custkey, 6000) OVER (PARTITION BY c_nationkey ORDER BY c_custkey NULLS LAST) AS j,
  SUM(c_acctbal) OVER (PARTITION BY c_nationkey) AS k,
  SUM(c_acctbal) OVER (ORDER BY c_custkey NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l,
  ROUND(AVG(c_acctbal) OVER (), 2) AS m,
  ROUND(
    AVG(c_acctbal) OVER (PARTITION BY c_nationkey ORDER BY c_custkey NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    2
  ) AS n,
  COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) OVER () AS o,
  COUNT(*) OVER () AS p
FROM tpch.customer
ORDER BY
  1
LIMIT 10
