WITH _t0 AS (
  SELECT
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_o_orderdate,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER),
    1
)
SELECT
  year_o_orderdate AS year,
  MAX(n_rows) AS best_month
FROM _t0
GROUP BY
  1
