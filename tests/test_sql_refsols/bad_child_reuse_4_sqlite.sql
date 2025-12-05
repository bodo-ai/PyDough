WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t AS (
  SELECT
    customer.c_acctbal,
    customer.c_custkey,
    _s1.n_rows,
    AVG(COALESCE(_s1.n_rows, 0)) OVER (PARTITION BY customer.c_nationkey) AS _w
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
)
SELECT
  c_custkey AS cust_key,
  n_rows AS n_orders
FROM _t
WHERE
  _w > COALESCE(n_rows, 0) AND n_rows <> 0
ORDER BY
  c_acctbal DESC
LIMIT 10
