WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year
  FROM tpch.orders
  GROUP BY
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
)
SELECT
  year,
  agg_0 AS current_year_orders,
  CAST((
    100.0 * (
      agg_0 - LAG(agg_0, 1) OVER (ORDER BY year)
    )
  ) AS REAL) / LAG(agg_0, 1) OVER (ORDER BY year) AS pct_change
FROM _t0
ORDER BY
  year
