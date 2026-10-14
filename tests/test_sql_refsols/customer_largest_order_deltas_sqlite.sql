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
), _s3 AS (
  SELECT
    anything_o_custkey,
    COALESCE(sum_r, 0) - LAG(COALESCE(sum_r, 0), 1) OVER (PARTITION BY anything_o_custkey ORDER BY anything_o_orderdate) AS revenue_delta
  FROM _t
  WHERE
    NOT _w IS NULL
)
SELECT
  MAX(customer.c_name) AS name,
  IIF(
    ABS(MIN(_s3.revenue_delta)) > MAX(_s3.revenue_delta),
    MIN(_s3.revenue_delta),
    MAX(_s3.revenue_delta)
  ) AS largest_diff
FROM tpch.customer AS customer
JOIN _s3 AS _s3
  ON _s3.anything_o_custkey = customer.c_custkey
WHERE
  customer.c_mktsegment = 'AUTOMOBILE'
GROUP BY
  _s3.anything_o_custkey
ORDER BY
  2 DESC
LIMIT 5
