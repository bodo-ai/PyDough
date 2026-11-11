SELECT
  supplier.s_name AS name,
  region.r_name AS region_name,
  supplier.s_acctbal - LAG(supplier.s_acctbal, 1) OVER (PARTITION BY nation.n_regionkey ORDER BY supplier.s_acctbal) AS acctbal_delta
FROM main.region AS region
JOIN main.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN main.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
ORDER BY
  3 DESC NULLS LAST
LIMIT 5
