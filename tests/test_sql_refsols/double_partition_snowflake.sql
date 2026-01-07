WITH _t0 AS (
  SELECT
    YEAR(CAST(o_orderdate AS TIMESTAMP)) AS year_o_orderdate,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1,
    MONTH(CAST(o_orderdate AS TIMESTAMP))
)
SELECT
  year_o_orderdate AS year,
  MAX(n_rows) AS best_month
FROM _t0
GROUP BY
  1
