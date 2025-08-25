WITH _t0 AS (
  SELECT
    CASE
      WHEN TRUNCATE(CAST(0.30000000000000004 * COUNT(orders.o_totalprice) OVER () AS FLOAT), 0) < ROW_NUMBER() OVER (ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_1
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1998
    AND customer.c_custkey = orders.o_custkey
)
SELECT
  MAX(expr_1) AS seventieth_order_price
FROM _t0
