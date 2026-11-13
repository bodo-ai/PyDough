WITH years AS (
  SELECT
    1992 + SEQ4() AS y
  FROM TABLE(GENERATOR(ROWCOUNT => 7)) AS _0
), years_2 AS (
  SELECT
    1992 + SEQ4() AS y
  FROM TABLE(GENERATOR(ROWCOUNT => 7)) AS _1
), _s3 AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice) AS sum_l_extendedprice
  FROM tpch.lineitem
  GROUP BY
    1
), _s5 AS (
  SELECT
    COALESCE(SUM(_s3.sum_l_extendedprice), 0) AS sum_order_total,
    years_2.y
  FROM years_2 AS years_2
  JOIN tpch.orders AS orders
    ON orders.o_custkey = 1 AND years_2.y >= YEAR(CAST(orders.o_orderdate AS TIMESTAMP))
  JOIN _s3 AS _s3
    ON _s3.l_orderkey = orders.o_orderkey
  GROUP BY
    2
)
SELECT
  years.y AS yy,
  COALESCE(_s5.sum_order_total, 0) AS total_to_date
FROM years AS years
LEFT JOIN _s5 AS _s5
  ON _s5.y = years.y
ORDER BY
  1 NULLS FIRST
