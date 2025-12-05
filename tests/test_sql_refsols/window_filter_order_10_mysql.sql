WITH _u_0 AS (
  SELECT
    c_custkey AS _u_1
  FROM tpch.CUSTOMER
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    1
), _t AS (
  SELECT
    ORDERS.o_totalprice,
    AVG(NULL) OVER () AS _w
  FROM tpch.ORDERS AS ORDERS
  LEFT JOIN _u_0 AS _u_0
    ON ORDERS.o_custkey = _u_0._u_1
  WHERE
    ORDERS.o_clerk = 'Clerk#000000001' AND _u_0._u_1 IS NULL
)
SELECT
  COUNT(*) AS n
FROM _t
WHERE
  o_totalprice < (
    0.05 * _w
  )
