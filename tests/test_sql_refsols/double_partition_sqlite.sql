WITH _t0 AS (
  SELECT
    COUNT() AS n_orders,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year
  FROM tpch.orders
  GROUP BY
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER),
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
)
SELECT
  year,
  MAX(n_orders) AS best_month
FROM _t0
GROUP BY
  year
