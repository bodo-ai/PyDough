WITH _s3 AS (
  SELECT
    1 AS expr_0,
    COALESCE(SUM(orders.o_totalprice), 0) AS total_spent,
    customer.c_custkey
  FROM tpch.customer AS customer
  LEFT JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
  WHERE
    customer.c_mktsegment = 'BUILDING'
  GROUP BY
    3
), _t AS (
  SELECT
    _s3.expr_0,
    orders.o_totalprice,
    AVG(_s3.total_spent) OVER () AS _w
  FROM tpch.orders AS orders
  LEFT JOIN _s3 AS _s3
    ON _s3.c_custkey = orders.o_custkey
  WHERE
    orders.o_clerk = 'Clerk#000000001'
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  expr_0 IS NULL AND o_totalprice < (
    0.05 * _w
  )
