WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    YEAR(CAST(o_orderdate AS TIMESTAMP))
)
SELECT
  MAX(n_rows) AS best_year
FROM _t0
