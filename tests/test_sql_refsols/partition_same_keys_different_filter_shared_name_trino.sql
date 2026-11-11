WITH _s0 AS (
  SELECT
    SUM(o_totalprice) AS sum_o_totalprice
  FROM mysql.tpch.orders
  WHERE
    YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1995
), _s1 AS (
  SELECT
    SUM(o_totalprice) AS sum_o_totalprice
  FROM mysql.tpch.orders
  WHERE
    YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1996
)
SELECT
  COALESCE(_s0.sum_o_totalprice, 0) AS total_1995,
  COALESCE(_s1.sum_o_totalprice, 0) AS total_1996
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
