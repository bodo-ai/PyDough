WITH _t0 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sale_price) AS agg_0,
    salesperson_id
  FROM main.sales
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sale_date, DAY) <= 30
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_t0.agg_1, 0) AS total_sales,
  COALESCE(_t0.agg_0, 0) AS total_revenue
FROM main.salespersons AS salespersons
JOIN _t0 AS _t0
  ON _t0.salesperson_id = salespersons._id
ORDER BY
  total_sales DESC
LIMIT 5
