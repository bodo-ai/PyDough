WITH _t0 AS (
  SELECT
    COUNT() AS n_orders
  FROM tpch.orders
  GROUP BY
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
)
SELECT
  MAX(n_orders) AS best_year
FROM _t0
