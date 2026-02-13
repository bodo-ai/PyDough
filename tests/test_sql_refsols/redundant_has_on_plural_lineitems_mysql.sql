WITH _u_0 AS (
  SELECT
    l_orderkey AS _u_1
  FROM tpch.LINEITEM
  WHERE
    l_quantity > 49
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.ORDERS AS ORDERS
LEFT JOIN _u_0 AS _u_0
  ON ORDERS.o_orderkey = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
