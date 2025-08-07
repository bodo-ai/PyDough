WITH _s0 AS (
  SELECT
    MIN(c_acctbal) AS smallest_bal
  FROM tpch.customer
)
SELECT
  nation.n_name AS name
FROM _s0 AS _s0
CROSS JOIN tpch.nation AS nation
WHERE
  _u_0._u_1 IS NULL
ORDER BY
  nation.n_name
