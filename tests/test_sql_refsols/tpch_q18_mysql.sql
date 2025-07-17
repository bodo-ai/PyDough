WITH _t0 AS (
  SELECT
    SUM(l_quantity) AS sum_l_quantity,
    l_orderkey
  FROM tpch.LINEITEM
  GROUP BY
    l_orderkey
)
SELECT
  CUSTOMER.c_name AS C_NAME,
  CUSTOMER.c_custkey AS C_CUSTKEY,
  ORDERS.o_orderkey AS O_ORDERKEY,
  ORDERS.o_orderdate AS O_ORDERDATE,
  ORDERS.o_totalprice AS O_TOTALPRICE,
  COALESCE(_t0.sum_l_quantity, 0) AS TOTAL_QUANTITY
FROM tpch.ORDERS AS ORDERS
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
JOIN _t0 AS _t0
  ON NOT _t0.sum_l_quantity IS NULL
  AND ORDERS.o_orderkey = _t0.l_orderkey
  AND _t0.sum_l_quantity > 300
ORDER BY
  ORDERS.o_totalprice DESC,
  ORDERS.o_orderdate
LIMIT 10
