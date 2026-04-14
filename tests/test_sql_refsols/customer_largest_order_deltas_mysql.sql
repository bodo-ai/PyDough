WITH _s1 AS (
  SELECT
    l_discount,
    l_extendedprice,
    l_orderkey
  FROM tpch.LINEITEM
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994 AND l_shipmode = 'AIR'
), _t5 AS (
  SELECT
    ANY_VALUE(ORDERS.o_custkey) AS anything_o_custkey,
    ANY_VALUE(ORDERS.o_orderdate) AS anything_o_orderdate,
    SUM(_s1.l_extendedprice * (
      1 - _s1.l_discount
    )) AS sum_r
  FROM tpch.ORDERS AS ORDERS
  LEFT JOIN _s1 AS _s1
    ON ORDERS.o_orderkey = _s1.l_orderkey
  WHERE
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1994
  GROUP BY
    ORDERS.o_orderkey
), _t AS (
  SELECT
    anything_o_custkey,
    anything_o_orderdate,
    sum_r,
    LAG(COALESCE(sum_r, 0), 1) OVER (PARTITION BY anything_o_custkey ORDER BY CASE WHEN anything_o_orderdate IS NULL THEN 1 ELSE 0 END, anything_o_orderdate) AS _w
  FROM _t5
), _t1 AS (
  SELECT
    _t.anything_o_custkey,
    CUSTOMER.c_name,
    COALESCE(_t.sum_r, 0) - LAG(COALESCE(_t.sum_r, 0), 1) OVER (PARTITION BY _t.anything_o_custkey ORDER BY CASE WHEN _t.anything_o_orderdate IS NULL THEN 1 ELSE 0 END, _t.anything_o_orderdate) AS revenue_delta
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN _t AS _t
    ON CUSTOMER.c_custkey = _t.anything_o_custkey AND NOT _t._w IS NULL
  WHERE
    CUSTOMER.c_mktsegment = 'AUTOMOBILE'
)
SELECT
  ANY_VALUE(c_name) AS name,
  CASE
    WHEN ABS(MIN(revenue_delta)) > MAX(revenue_delta)
    THEN MIN(revenue_delta)
    ELSE MAX(revenue_delta)
  END AS largest_diff
FROM _t1
GROUP BY
  anything_o_custkey
ORDER BY
  2 DESC
LIMIT 5
