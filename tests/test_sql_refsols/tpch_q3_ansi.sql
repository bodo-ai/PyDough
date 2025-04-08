WITH _t2_2 AS (
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
    ON lineitem.l_orderkey = orders.o_orderkey
    AND lineitem.l_shipdate > CAST('1995-03-15' AS DATE)
  WHERE
    orders.o_orderdate < CAST('1995-03-15' AS DATE)
  GROUP BY
    orders.o_orderdate,
    lineitem.l_orderkey,
    orders.o_shippriority
), _t0_2 AS (
  SELECT
    order_key AS l_orderkey,
    order_date AS o_orderdate,
    ship_priority AS o_shippriority,
    COALESCE(agg_0, 0) AS revenue,
    COALESCE(agg_0, 0) AS ordering_1,
    order_date AS ordering_2,
    order_key AS ordering_3
  FROM _t2_2
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3
  LIMIT 10
)
SELECT
  l_orderkey AS L_ORDERKEY,
  revenue AS REVENUE,
  o_orderdate AS O_ORDERDATE,
  o_shippriority AS O_SHIPPRIORITY
FROM _t0_2
ORDER BY
  ordering_1 DESC,
  ordering_2,
  ordering_3
