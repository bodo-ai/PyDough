WITH _t0 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_orderkey AS order_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey
)
SELECT
  customer.c_name AS C_NAME,
  customer.c_custkey AS C_CUSTKEY,
  orders.o_orderkey AS O_ORDERKEY,
  orders.o_orderdate AS O_ORDERDATE,
  orders.o_totalprice AS O_TOTALPRICE,
  COALESCE(_t0.agg_0, 0) AS TOTAL_QUANTITY
FROM tpch.orders AS orders
JOIN tpch.customer AS customer
  ON customer.c_custkey = orders.o_custkey
JOIN _t0 AS _t0
  ON NOT _t0.agg_0 IS NULL AND _t0.agg_0 > 300 AND _t0.order_key = orders.o_orderkey
ORDER BY
  o_totalprice DESC,
  o_orderdate
LIMIT 10
