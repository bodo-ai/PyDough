WITH _t3 AS (
  SELECT
    SUM(l_quantity) AS agg_0,
    l_orderkey AS order_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey
), _t0_2 AS (
  SELECT
    customer.c_custkey,
    customer.c_name,
    orders.o_orderdate,
    orders.o_orderkey,
    orders.o_totalprice,
    COALESCE(_t3.agg_0, 0) AS total_quantity,
    orders.o_totalprice AS ordering_1,
    orders.o_orderdate AS ordering_2
  FROM tpch.orders AS orders
  LEFT JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  LEFT JOIN _t3 AS _t3
    ON _t3.order_key = orders.o_orderkey
  WHERE
    NOT _t3.agg_0 IS NULL AND _t3.agg_0 > 300
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 10
)
SELECT
  c_name AS C_NAME,
  c_custkey AS C_CUSTKEY,
  o_orderkey AS O_ORDERKEY,
  o_orderdate AS O_ORDERDATE,
  o_totalprice AS O_TOTALPRICE,
  total_quantity AS TOTAL_QUANTITY
FROM _t0_2
ORDER BY
  ordering_1 DESC,
  ordering_2
