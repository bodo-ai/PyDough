WITH _t0 AS (
  SELECT
    supplier.s_acctbal - LAG(supplier.s_acctbal, 1) OVER (PARTITION BY region.r_regionkey ORDER BY supplier.s_acctbal) AS acctbal_delta,
    supplier.s_name AS name_8,
    region.r_name AS region_name
  FROM tpch.region AS region
  JOIN tpch.nation AS nation
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.supplier AS supplier
    ON nation.n_nationkey = supplier.s_nationkey
)
SELECT
  name_8 AS name,
  region_name,
  acctbal_delta
FROM _t0
ORDER BY
  acctbal_delta DESC
LIMIT 5
