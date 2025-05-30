WITH _t0 AS (
  SELECT
    COUNT() AS n_orders
  FROM tpch.orders
  GROUP BY
    EXTRACT(YEAR FROM o_orderdate)
)
SELECT
  MAX(n_orders) AS best_year
FROM _t0
