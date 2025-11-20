WITH _t1 AS (
  SELECT
    l_orderkey,
    SUM(l_quantity) AS sum_lquantity
  FROM tpch.LINEITEM
  GROUP BY
    1
)
SELECT
  CUSTOMER.c_name AS C_NAME,
  CUSTOMER.c_custkey AS C_CUSTKEY,
  ORDERS.o_orderkey AS O_ORDERKEY,
  ORDERS.o_orderdate AS O_ORDERDATE,
  ORDERS.o_totalprice AS O_TOTALPRICE,
  _t1.sum_lquantity AS TOTAL_QUANTITY
FROM tpch.ORDERS AS ORDERS
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = ORDERS.o_custkey
JOIN _t1 AS _t1
  ON NOT _t1.sum_lquantity IS NULL
  AND ORDERS.o_orderkey = _t1.l_orderkey
  AND _t1.sum_lquantity > 300
ORDER BY
  5 DESC,
  4
LIMIT 10
