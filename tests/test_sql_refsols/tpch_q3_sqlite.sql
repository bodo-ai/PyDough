WITH _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS sum_expr_1,
    lineitem.l_orderkey,
    orders.o_orderdate,
    orders.o_shippriority
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_shipdate > '1995-03-15'
  WHERE
    orders.o_orderdate < '1995-03-15'
  GROUP BY
    lineitem.l_orderkey,
    orders.o_orderdate,
    orders.o_shippriority
)
SELECT
  l_orderkey AS L_ORDERKEY,
  COALESCE(sum_expr_1, 0) AS REVENUE,
  o_orderdate AS O_ORDERDATE,
  o_shippriority AS O_SHIPPRIORITY
FROM _t1
ORDER BY
  revenue DESC,
  o_orderdate,
  l_orderkey
LIMIT 10
