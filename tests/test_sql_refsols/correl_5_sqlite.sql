WITH _s0 AS (
  SELECT
    MIN(s_acctbal) AS smallest_bal
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
    _s0.smallest_bal + 4.0
  )
GROUP BY
  region.r_regionkey
ORDER BY
  name
