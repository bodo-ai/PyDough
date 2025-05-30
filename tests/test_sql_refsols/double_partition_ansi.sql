WITH _t0 AS (
  SELECT
    COUNT() AS n_orders,
    EXTRACT(YEAR FROM o_orderdate) AS year
  FROM tpch.orders
  GROUP BY
    EXTRACT(MONTH FROM o_orderdate),
    EXTRACT(YEAR FROM o_orderdate)
)
SELECT
  year,
  MAX(n_orders) AS best_month
FROM _t0
GROUP BY
  year
