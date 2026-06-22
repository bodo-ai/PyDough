WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _t1 AS (
  SELECT
    customer.c_custkey,
    _s1.n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
  ORDER BY
    customer.c_acctbal DESC,
    COALESCE(_s1.n_rows, 0)
  LIMIT 10
)
SELECT
  c_custkey AS cust_key,
  n_rows AS n_orders
FROM _t1
WHERE
  n_rows <> 0
ORDER BY
  1
