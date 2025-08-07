WITH _s0 AS (
  SELECT
    MIN(c_acctbal) AS smallest_bal
  FROM tpch.customer
)
SELECT
  nation.n_name AS name
FROM _s0 AS _s0
CROSS JOIN tpch.nation AS nation
JOIN tpch.customer AS customer
  ON customer.c_acctbal <= (
    _s0.smallest_bal + 5.0
  )
  AND customer.c_nationkey = nation.n_nationkey
ORDER BY
  nation.n_name
