WITH _t2 AS (
  SELECT
    o_custkey
  FROM tpch.orders
), _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    1
), _s2 AS (
  SELECT
    customer.c_acctbal,
    customer.c_custkey,
    _s1.n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
  ORDER BY
    1 DESC,
    2 DESC
  LIMIT 10
)
SELECT
  c_custkey AS cust_key,
  COALESCE(n_rows, 0) AS n_orders
FROM _s2
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t2
    WHERE
      _s2.c_custkey = o_custkey
  )
ORDER BY
  c_acctbal DESC,
  1 DESC
