WITH _s3 AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice) AS sum_l_extendedprice
  FROM tpch.lineitem
  GROUP BY
    1
), _s5 AS (
  SELECT
    COALESCE(SUM(_s3.sum_l_extendedprice), 0) AS sum_order_total,
    column1 AS y
  FROM (VALUES
    (1992),
    (1993),
    (1994),
    (1995),
    (1996),
    (1997),
    (1998)) AS years_2(y)
  JOIN tpch.orders AS orders
    ON column1 >= EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME))
    AND orders.o_custkey = 1
  JOIN _s3 AS _s3
    ON _s3.l_orderkey = orders.o_orderkey
  GROUP BY
    2
)
SELECT
  column1 AS yy,
  COALESCE(_s5.sum_order_total, 0) AS total_to_date
FROM (VALUES
  (1992),
  (1993),
  (1994),
  (1995),
  (1996),
  (1997),
  (1998)) AS years(y)
LEFT JOIN _s5 AS _s5
  ON _s5.y = column1
ORDER BY
  1
