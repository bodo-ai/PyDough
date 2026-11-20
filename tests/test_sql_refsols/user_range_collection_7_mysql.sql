WITH _s3 AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice) AS sum_l_extendedprice
  FROM tpch.LINEITEM
  GROUP BY
    1
), _s5 AS (
  SELECT
    COALESCE(SUM(_s3.sum_l_extendedprice), 0) AS sum_order_total,
    years_2.y
  FROM (VALUES
    ROW(1992),
    ROW(1993),
    ROW(1994),
    ROW(1995),
    ROW(1996),
    ROW(1997),
    ROW(1998)) AS years_2(y)
  JOIN tpch.ORDERS AS ORDERS
    ON ORDERS.o_custkey = 1
    AND years_2.y >= EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME))
  JOIN _s3 AS _s3
    ON ORDERS.o_orderkey = _s3.l_orderkey
  GROUP BY
    2
)
SELECT
  years.y AS yy,
  COALESCE(_s5.sum_order_total, 0) AS total_to_date
FROM (VALUES
  ROW(1992),
  ROW(1993),
  ROW(1994),
  ROW(1995),
  ROW(1996),
  ROW(1997),
  ROW(1998)) AS years(y)
LEFT JOIN _s5 AS _s5
  ON _s5.y = years.y
ORDER BY
  1
