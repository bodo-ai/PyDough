WITH _s0 AS (
  SELECT
    MIN(s_acctbal) AS min_s_acctbal
  FROM tpch.supplier
)
SELECT
  MAX(region.r_name) AS name
FROM _s0 AS _s0
CROSS JOIN tpch.region AS region
JOIN tpch.nation AS nation
  ON nation.n_regionkey = region.r_regionkey
JOIN tpch.supplier AS supplier
  ON nation.n_nationkey = supplier.s_nationkey
  AND supplier.s_acctbal <= (
    _s0.min_s_acctbal + 4.0
  )
GROUP BY
  region.r_regionkey
ORDER BY
  1
