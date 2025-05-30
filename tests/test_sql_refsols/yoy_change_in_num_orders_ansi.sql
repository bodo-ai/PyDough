WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    EXTRACT(YEAR FROM o_orderdate) AS year
  FROM tpch.orders
  GROUP BY
    EXTRACT(YEAR FROM o_orderdate)
)
SELECT
  year,
  agg_0 AS current_year_orders,
  (
    100.0 * (
      agg_0 - LAG(agg_0, 1) OVER (ORDER BY year NULLS LAST)
    )
  ) / LAG(agg_0, 1) OVER (ORDER BY year NULLS LAST) AS pct_change
FROM _t0
ORDER BY
  year
