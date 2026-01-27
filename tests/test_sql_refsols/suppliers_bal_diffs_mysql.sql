SELECT
  SUPPLIER.s_name AS name,
  REGION.r_name AS region_name,
  SUPPLIER.s_acctbal - LAG(SUPPLIER.s_acctbal, 1) OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN SUPPLIER.s_acctbal IS NULL THEN 1 ELSE 0 END, SUPPLIER.s_acctbal) AS acctbal_delta
FROM tpch.REGION AS REGION
JOIN tpch.NATION AS NATION
  ON NATION.n_regionkey = REGION.r_regionkey
JOIN tpch.SUPPLIER AS SUPPLIER
  ON NATION.n_nationkey = SUPPLIER.s_nationkey
ORDER BY
  3 DESC
LIMIT 5
