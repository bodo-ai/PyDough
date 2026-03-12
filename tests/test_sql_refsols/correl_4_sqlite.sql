WITH _s0 AS (
  SELECT
    MIN(c_acctbal) AS min_c_acctbal
  FROM tpch.customer
)
SELECT
  n_name AS name
FROM tpch.nation
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s0 AS _s0
    CROSS JOIN tpch.nation AS nation
    JOIN tpch.customer AS customer
      ON customer.c_acctbal <= (
        _s0.min_c_acctbal + 5.0
      )
      AND customer.c_nationkey = nation.n_nationkey
    WHERE
      nation.n_nationkey = nation.n_nationkey
  )
ORDER BY
  1
