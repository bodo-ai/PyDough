WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
)
SELECT
  MAX(n_rows) AS best_year
FROM _t0
