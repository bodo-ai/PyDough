WITH _s3 AS (
  SELECT
    1 AS expr_0,
    COALESCE(SUM(ORDERS.o_totalprice), 0) AS total_spent,
    CUSTOMER.c_custkey
  FROM tpch.CUSTOMER AS CUSTOMER
  LEFT JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
  WHERE
    CUSTOMER.c_mktsegment = 'BUILDING'
  GROUP BY
    3
), _t AS (
  SELECT
    _s3.expr_0,
    ORDERS.o_totalprice,
    AVG(_s3.total_spent) OVER () AS _w
  FROM tpch.ORDERS AS ORDERS
  LEFT JOIN _s3 AS _s3
    ON ORDERS.o_custkey = _s3.c_custkey
  WHERE
    ORDERS.o_clerk = 'Clerk#000000001'
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  expr_0 IS NULL AND o_totalprice < (
    0.05 * _w
  )
