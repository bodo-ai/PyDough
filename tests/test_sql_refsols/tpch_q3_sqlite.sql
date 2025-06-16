WITH _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    orders.o_orderdate AS order_date,
    lineitem.l_orderkey AS order_key,
    orders.o_shippriority AS ship_priority
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
  order_key AS L_ORDERKEY,
  COALESCE(agg_0, 0) AS REVENUE,
  order_date AS O_ORDERDATE,
  ship_priority AS O_SHIPPRIORITY
FROM _t1
ORDER BY
  revenue DESC,
  o_orderdate,
  l_orderkey
LIMIT 10
