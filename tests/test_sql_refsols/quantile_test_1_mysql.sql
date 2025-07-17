WITH _t0 AS (
  SELECT
    CASE
      WHEN CAST(0.30000000000000004 * COUNT(ORDERS.o_totalprice) OVER () AS SIGNED) < ROW_NUMBER() OVER (ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_1
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey AND YEAR(ORDERS.o_orderdate) = 1998
)
SELECT
  MAX(expr_1) AS seventieth_order_price
FROM _t0
