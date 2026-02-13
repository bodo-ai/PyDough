WITH _t0 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year_o_orderdate,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
)
SELECT
  year_o_orderdate AS year,
  n_rows AS current_year_orders,
  (
    100.0 * CAST(n_rows - LAG(n_rows, 1) OVER (ORDER BY CASE WHEN year_o_orderdate IS NULL THEN 1 ELSE 0 END, year_o_orderdate) AS DOUBLE)
  ) / LAG(n_rows, 1) OVER (ORDER BY CASE WHEN year_o_orderdate IS NULL THEN 1 ELSE 0 END, year_o_orderdate) AS pct_change
FROM _t0
ORDER BY
  1
