WITH _u_0 AS (
  SELECT
    l_orderkey AS _u_1
  FROM tpch.lineitem
  WHERE
    l_quantity > 49
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.orders AS orders
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = orders.o_orderkey
WHERE
  NOT _u_0._u_1 IS NULL
