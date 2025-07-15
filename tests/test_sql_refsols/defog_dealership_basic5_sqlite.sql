WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows_1,
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
  _s1.n_rows_1 AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  _s1.n_rows_1 DESC
LIMIT 5
