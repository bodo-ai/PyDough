SELECT
  PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY
    orders.o_totalprice NULLS LAST) AS seventieth_order_price
FROM tpch.customer AS customer
JOIN tpch.orders AS orders
  ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1998
  AND customer.c_custkey = orders.o_custkey
