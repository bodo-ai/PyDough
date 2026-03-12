WITH _u_0 AS (
  SELECT
    c_custkey AS _u_1
  FROM tpch.customer
  WHERE
    c_mktsegment = 'BUILDING'
  GROUP BY
    1
), _t1 AS (
  SELECT
    1 AS "_"
  FROM tpch.orders AS orders
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = orders.o_custkey
  WHERE
    _u_0._u_1 IS NULL AND orders.o_clerk = 'Clerk#000000001'
  QUALIFY
    orders.o_totalprice < (
      0.05 * AVG(CAST(NULL AS INT)) OVER ()
    )
)
SELECT
  COUNT(*) AS n
FROM _t1
