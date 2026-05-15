WITH _s1 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_orderkey
  FROM tpch.lineitem
  WHERE
    YEAR(CAST(l_shipdate AS TIMESTAMP)) = 1994 AND l_shipmode = 'AIR'
), _t5 AS (
  SELECT
    ANY_VALUE(orders.o_custkey) AS anything_o_custkey,
    ANY_VALUE(orders.o_orderdate) AS anything_o_orderdate,
    SUM(_s1.l_extendedprice * (
      1 - _s1.l_discount
    )) AS sum_r
  FROM tpch.orders AS orders
  LEFT JOIN _s1 AS _s1
    ON _s1.l_orderkey = orders.o_orderkey
  WHERE
    YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1994
  GROUP BY
    orders.o_orderkey
), _t4 AS (
  SELECT
    anything_o_custkey,
    anything_o_orderdate,
    sum_r
  FROM _t5
  QUALIFY
    NOT LAG(COALESCE(sum_r, 0), 1) OVER (PARTITION BY anything_o_custkey ORDER BY anything_o_orderdate) IS NULL
), _t1 AS (
  SELECT
    _t4.anything_o_custkey,
    customer.c_name,
    COALESCE(_t4.sum_r, 0) - LAG(COALESCE(_t4.sum_r, 0), 1) OVER (PARTITION BY _t4.anything_o_custkey ORDER BY _t4.anything_o_orderdate) AS revenue_delta
  FROM tpch.customer AS customer
  JOIN _t4 AS _t4
    ON _t4.anything_o_custkey = customer.c_custkey
  WHERE
    customer.c_mktsegment = 'AUTOMOBILE'
)
SELECT
  ANY_VALUE(c_name) AS name,
  IFF(ABS(MIN(revenue_delta)) > MAX(revenue_delta), MIN(revenue_delta), MAX(revenue_delta)) AS largest_diff
FROM _t1
GROUP BY
  anything_o_custkey
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
