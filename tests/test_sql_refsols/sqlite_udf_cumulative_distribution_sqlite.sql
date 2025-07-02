WITH _t0 AS (
  SELECT
    ROUND(CUME_DIST() OVER (ORDER BY orders.o_orderpriority), 4) AS c
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  WHERE
    (
      CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1992
      OR CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    )
    AND (
      CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
      OR orders.o_orderpriority = '2-HIGH'
    )
)
SELECT
  c,
  COUNT(*) AS n
FROM _t0
GROUP BY
  c
ORDER BY
  c
