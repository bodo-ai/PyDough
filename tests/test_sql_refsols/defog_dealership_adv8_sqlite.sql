WITH _s3 AS (
  SELECT
    DATE(sales.sale_date, 'start of month') AS sale_month,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM main.sales AS sales
  JOIN main.salespersons AS salespersons
    ON CAST(STRFTIME('%Y', salespersons.hire_date) AS INTEGER) <= 2023
    AND CAST(STRFTIME('%Y', salespersons.hire_date) AS INTEGER) >= 2022
    AND sales.salesperson_id = salespersons._id
  GROUP BY
    1
)
SELECT
  STRFTIME('%Y-%m-%d', DATE(months_range.column1, 'start of month')) AS month,
  COALESCE(_s3.n_rows, 0) AS PMSPS,
  COALESCE(_s3.sum_sale_price, 0) AS PMSR
FROM (VALUES
  ('2025-10-01 00:00:00'),
  ('2025-11-01 00:00:00'),
  ('2025-12-01 00:00:00'),
  ('2026-01-01 00:00:00'),
  ('2026-02-01 00:00:00'),
  ('2026-03-01 00:00:00')) AS months_range
LEFT JOIN _s3 AS _s3
  ON _s3.sale_month = DATE(months_range.column1, 'start of month')
ORDER BY
  DATE(months_range.column1, 'start of month')
