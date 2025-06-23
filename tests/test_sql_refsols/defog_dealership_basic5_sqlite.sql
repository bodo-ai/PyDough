WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_1,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sale_date, 'start of day'))
    ) AS INTEGER) <= 30
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  _t0.agg_1 AS total_sales,
  COALESCE(_t0.sum_sale_price, 0) AS total_revenue
FROM main.salespersons AS salespersons
JOIN _t0 AS _t0
  ON _t0.salesperson_id = salespersons._id
ORDER BY
  total_sales DESC
LIMIT 5
