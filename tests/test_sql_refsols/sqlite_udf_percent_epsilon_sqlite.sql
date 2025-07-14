WITH _t1 AS (
  SELECT
    ABS(AVG(o_totalprice) OVER () - o_totalprice) <= 1 AS expr_5,
    ABS(AVG(o_totalprice) OVER () - o_totalprice) <= 10 AS expr_6,
    ABS(AVG(o_totalprice) OVER () - o_totalprice) <= 100 AS expr_7,
    ABS(AVG(o_totalprice) OVER () - o_totalprice) <= 1000 AS expr_8,
    ABS(AVG(o_totalprice) OVER () - o_totalprice) <= 10000 AS expr_9
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1992
)
SELECT
  ROUND(CAST((
    100.0 * SUM(CASE WHEN expr_5 THEN 1 END)
  ) AS REAL) / COUNT(*), 4) AS pct_e1,
  ROUND(CAST((
    100.0 * SUM(CASE WHEN expr_6 THEN 1 END)
  ) AS REAL) / COUNT(*), 4) AS pct_e10,
  ROUND(CAST((
    100.0 * SUM(CASE WHEN expr_7 THEN 1 END)
  ) AS REAL) / COUNT(*), 4) AS pct_e100,
  ROUND(CAST((
    100.0 * SUM(CASE WHEN expr_8 THEN 1 END)
  ) AS REAL) / COUNT(*), 4) AS pct_e1000,
  ROUND(CAST((
    100.0 * SUM(CASE WHEN expr_9 THEN 1 END)
  ) AS REAL) / COUNT(*), 4) AS pct_e10000
FROM _t1
