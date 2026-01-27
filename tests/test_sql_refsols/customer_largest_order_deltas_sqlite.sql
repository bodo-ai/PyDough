WITH _s1 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_orderkey
  FROM tpch.lineitem
  WHERE
    CAST(STRFTIME('%Y', l_shipdate) AS INTEGER) = 1994 AND l_shipmode = 'AIR'
), _t5 AS (
  SELECT
    MAX(orders.o_custkey) AS anything_o_custkey,
    MAX(orders.o_orderdate) AS anything_o_orderdate,
    SUM(_s1.l_extendedprice * (
      1 - _s1.l_discount
    )) AS sum_r
  FROM tpch.orders AS orders
  LEFT JOIN _s1 AS _s1
    ON _s1.l_orderkey = orders.o_orderkey
  WHERE
    CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1994
  GROUP BY
    orders.o_orderkey
), _t AS (
  SELECT
    anything_o_custkey,
    anything_o_orderdate,
    sum_r,
    LAG(COALESCE(sum_r, 0), 1) OVER (PARTITION BY anything_o_custkey ORDER BY anything_o_orderdate) AS _w
  FROM _t5
), _t1 AS (
  SELECT
    _t.anything_o_custkey,
    customer.c_name,
    COALESCE(_t.sum_r, 0) - LAG(COALESCE(_t.sum_r, 0), 1) OVER (PARTITION BY _t.anything_o_custkey ORDER BY _t.anything_o_orderdate) AS revenue_delta
  FROM tpch.customer AS customer
  JOIN _t AS _t
    ON NOT _t._w IS NULL AND _t.anything_o_custkey = customer.c_custkey
  WHERE
    customer.c_mktsegment = 'AUTOMOBILE'
)
SELECT
  MAX(c_name) AS name,
  IIF(ABS(MIN(revenue_delta)) > MAX(revenue_delta), MIN(revenue_delta), MAX(revenue_delta)) AS largest_diff
FROM _t1
GROUP BY
  anything_o_custkey
ORDER BY
  2 DESC
LIMIT 5
