WITH _t1 AS (
  SELECT
    l_orderkey,
    SUM(l_quantity) AS sum_l_quantity
  FROM tpch.lineitem
  GROUP BY
    1
)
SELECT
  customer.c_name AS C_NAME,
  customer.c_custkey AS C_CUSTKEY,
  orders.o_orderkey AS O_ORDERKEY,
  orders.o_orderdate AS O_ORDERDATE,
  orders.o_totalprice AS O_TOTALPRICE,
  _t1.sum_l_quantity AS TOTAL_QUANTITY
FROM tpch.orders AS orders
JOIN tpch.customer AS customer
  ON customer.c_custkey = orders.o_custkey
JOIN _t1 AS _t1
  ON NOT _t1.sum_l_quantity IS NULL
  AND _t1.l_orderkey = orders.o_orderkey
  AND _t1.sum_l_quantity > 300
ORDER BY
  5 DESC,
  4
LIMIT 10
