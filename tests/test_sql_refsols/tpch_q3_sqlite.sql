WITH _t1 AS (
  SELECT
    SUM(_s4.l_extendedprice * (
      1 - _s4.l_discount
    )) AS agg_0,
    _s0.o_orderdate AS order_date,
    _s4.l_orderkey AS order_key,
    _s0.o_shippriority AS ship_priority
  FROM tpch.orders AS _s0
  JOIN tpch.customer AS _s1
    ON _s0.o_custkey = _s1.c_custkey AND _s1.c_mktsegment = 'BUILDING'
  JOIN tpch.lineitem AS _s4
    ON _s0.o_orderkey = _s4.l_orderkey AND _s4.l_shipdate > '1995-03-15'
  WHERE
    _s0.o_orderdate < '1995-03-15'
  GROUP BY
    _s0.o_orderdate,
    _s4.l_orderkey,
    _s0.o_shippriority
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
