WITH _s0 AS (
  SELECT
    MIN(c_acctbal) AS min_c_acctbal
  FROM tpch.customer
), _u_0 AS (
  SELECT
    nation.n_nationkey AS _u_1
  FROM _s0 AS _s0
  CROSS JOIN tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_acctbal <= (
      _s0.min_c_acctbal + 5.0
    )
    AND customer.c_nationkey = nation.n_nationkey
  GROUP BY
    1
)
SELECT
  nation.n_name AS name
FROM tpch.nation AS nation
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = nation.n_nationkey
WHERE
  _u_0._u_1 IS NULL
ORDER BY
  1
