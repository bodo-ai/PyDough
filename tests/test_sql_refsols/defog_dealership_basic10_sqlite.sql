WITH _s1 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    COUNT() AS agg_1,
    salesperson_id
  FROM main.sales
  WHERE
    sale_date >= DATETIME('now', '-3 month')
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_s1.agg_1, 0) AS total_sales,
  COALESCE(_s1.agg_0, 0) AS total_revenue
FROM main.salespersons AS salespersons
LEFT JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  total_revenue DESC
LIMIT 3
