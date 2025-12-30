WITH _u_0 AS (
  SELECT
    c_custkey AS _u_1
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    1
), _t AS (
  SELECT
    orders.o_totalprice,
    AVG(CAST(NULL AS INTEGER)) OVER () AS _w
  FROM tpch.orders AS orders
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = orders.o_custkey
  WHERE
    _u_0._u_1 IS NULL AND orders.o_clerk = 'Clerk#000000001'
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  o_totalprice < (
    0.05 * _w
  )
