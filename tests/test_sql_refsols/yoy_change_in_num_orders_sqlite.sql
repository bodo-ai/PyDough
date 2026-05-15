WITH _t0 AS (
  SELECT
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_o_orderdate,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  year_o_orderdate AS year,
  n_rows AS current_year_orders,
  CAST((
    100.0 * CAST(n_rows - LAG(n_rows, 1) OVER (ORDER BY year_o_orderdate) AS REAL)
  ) AS REAL) / LAG(n_rows, 1) OVER (ORDER BY year_o_orderdate) AS pct_change
FROM _t0
ORDER BY
  1
