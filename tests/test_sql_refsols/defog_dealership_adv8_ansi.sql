WITH _s3 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(sales.sale_date AS TIMESTAMP)) AS sale_month,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM main.sales AS sales
  JOIN main.salespersons AS salespersons
    ON EXTRACT(YEAR FROM CAST(salespersons.hire_date AS DATETIME)) <= 2023
    AND EXTRACT(YEAR FROM CAST(salespersons.hire_date AS DATETIME)) >= 2022
    AND sales.salesperson_id = salespersons._id
  GROUP BY
    1
)
SELECT
  TIME_TO_STR(DATE_TRUNC('MONTH', CAST(column1 AS TIMESTAMP)), '%Y-%m-%d') AS month,
  COALESCE(_s3.n_rows, 0) AS PMSPS,
  COALESCE(_s3.sum_sale_price, 0) AS PMSR
FROM (VALUES
  (CAST('2025-10-01 00:00:00' AS TIMESTAMP)),
  (CAST('2025-11-01 00:00:00' AS TIMESTAMP)),
  (CAST('2025-12-01 00:00:00' AS TIMESTAMP)),
  (CAST('2026-01-01 00:00:00' AS TIMESTAMP)),
  (CAST('2026-02-01 00:00:00' AS TIMESTAMP)),
  (CAST('2026-03-01 00:00:00' AS TIMESTAMP))) AS months_range(dt)
LEFT JOIN _s3 AS _s3
  ON _s3.sale_month = DATE_TRUNC('MONTH', CAST(column1 AS TIMESTAMP))
ORDER BY
  DATE_TRUNC('MONTH', CAST(column1 AS TIMESTAMP))
