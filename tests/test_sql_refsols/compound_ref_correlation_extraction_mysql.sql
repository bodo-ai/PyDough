WITH _s1 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year_o_orderdate
  FROM tpch.ORDERS
), _s5 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS year_o_orderdate,
    SUM(LINEITEM.l_extendedprice) AS sum_l_extendedprice
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_orderkey = ORDERS.o_orderkey
  GROUP BY
    1
)
SELECT
  _s1.year_o_orderdate AS year,
  COALESCE(_s5.sum_l_extendedprice, 0) AS bug
FROM tpch.CUSTOMER AS CUSTOMER
CROSS JOIN _s1 AS _s1
LEFT JOIN _s5 AS _s5
  ON _s1.year_o_orderdate = _s5.year_o_orderdate
ORDER BY
  2 DESC
LIMIT 5
