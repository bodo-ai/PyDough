WITH _s1 AS (
  SELECT DISTINCT
    YEAR(CAST(o_orderdate AS TIMESTAMP)) AS year_o_orderdate
  FROM tpch.orders
), _s5 AS (
  SELECT
    YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) AS year_o_orderdate,
    SUM(lineitem.l_extendedprice) AS sum_l_extendedprice
  FROM tpch.orders AS orders
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    1
)
SELECT
  _s1.year_o_orderdate AS year,
  COALESCE(_s5.sum_l_extendedprice, 0) AS bug
FROM tpch.customer AS customer
CROSS JOIN _s1 AS _s1
LEFT JOIN _s5 AS _s5
  ON _s1.year_o_orderdate = _s5.year_o_orderdate
ORDER BY
  2 DESC
LIMIT 5
