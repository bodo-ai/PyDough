SELECT
  customer.c_custkey AS key,
  ROW_NUMBER() OVER (ORDER BY customer.c_acctbal NULLS LAST, customer.c_custkey NULLS LAST) AS a,
  ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal NULLS LAST, customer.c_custkey NULLS LAST) AS b,
  RANK() OVER (ORDER BY customer.c_mktsegment NULLS LAST) AS c,
  DENSE_RANK() OVER (ORDER BY customer.c_mktsegment NULLS LAST) AS d,
  NTILE(100) OVER (ORDER BY customer.c_acctbal NULLS LAST, customer.c_custkey NULLS LAST) AS e,
  NTILE(12) OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal NULLS LAST, customer.c_custkey NULLS LAST) AS f,
  LAG(customer.c_custkey, 1) OVER (ORDER BY customer.c_custkey NULLS LAST) AS g,
  LAG(customer.c_custkey, 2, -1) OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_custkey NULLS LAST) AS h,
  LEAD(customer.c_custkey, 1) OVER (ORDER BY customer.c_custkey NULLS LAST) AS i,
  LEAD(customer.c_custkey, 6000) OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_custkey NULLS LAST) AS j,
  SUM(customer.c_acctbal) OVER (PARTITION BY customer.c_nationkey) AS k,
  SUM(customer.c_acctbal) OVER (ORDER BY customer.c_custkey NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l,
  ROUND(AVG(customer.c_acctbal) OVER (), 2) AS m,
  ROUND(
    AVG(customer.c_acctbal) OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_custkey NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    2
  ) AS n,
  COUNT(CASE WHEN customer.c_acctbal > 0 THEN customer.c_acctbal ELSE NULL END) OVER () AS o,
  COUNT(*) OVER () AS p
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
ORDER BY
  1
LIMIT 10
