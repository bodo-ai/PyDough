WITH _t2 AS (
  SELECT
    AVG(o_totalprice) OVER () AS global_avg,
    o_totalprice
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1992
)
SELECT
  ROUND(
    CAST((
      100.0 * SUM(CASE WHEN ABS(o_totalprice - global_avg) <= 1 THEN 1 END)
    ) AS REAL) / COUNT(*),
    4
  ) AS pct_e1,
  ROUND(
    CAST((
      100.0 * SUM(CASE WHEN ABS(o_totalprice - global_avg) <= 10 THEN 1 END)
    ) AS REAL) / COUNT(*),
    4
  ) AS pct_e10,
  ROUND(
    CAST((
      100.0 * SUM(CASE WHEN ABS(o_totalprice - global_avg) <= 100 THEN 1 END)
    ) AS REAL) / COUNT(*),
    4
  ) AS pct_e100,
  ROUND(
    CAST((
      100.0 * SUM(CASE WHEN ABS(o_totalprice - global_avg) <= 1000 THEN 1 END)
    ) AS REAL) / COUNT(*),
    4
  ) AS pct_e1000,
  ROUND(
    CAST((
      100.0 * SUM(CASE WHEN ABS(o_totalprice - global_avg) <= 10000 THEN 1 END)
    ) AS REAL) / COUNT(*),
    4
  ) AS pct_e10000
FROM _t2
