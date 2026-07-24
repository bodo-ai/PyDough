WITH _s3 AS (
  SELECT
    TRUNC(CAST(sales.sale_date AS TIMESTAMP), 'MONTH') AS sale_month,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM defog.dealership.sales AS sales
  JOIN defog.dealership.salespersons AS salespersons
    ON EXTRACT(YEAR FROM CAST(salespersons.hire_date AS TIMESTAMP)) <= 2023
    AND EXTRACT(YEAR FROM CAST(salespersons.hire_date AS TIMESTAMP)) >= 2022
    AND sales.salesperson_id = salespersons.id
  GROUP BY
    1
)
SELECT
  DATE_FORMAT(TRUNC(months_range.dt, 'MONTH'), 'yyyy-MM-dd') AS month,
  COALESCE(_s3.n_rows, 0) AS PMSPS,
  COALESCE(_s3.sum_sale_price, 0) AS PMSR
FROM VALUES
  (CAST('2025-10-01 00:00:00' AS TIMESTAMP)),
  (CAST('2025-11-01 00:00:00' AS TIMESTAMP)),
  (CAST('2025-12-01 00:00:00' AS TIMESTAMP)),
  (CAST('2026-01-01 00:00:00' AS TIMESTAMP)),
  (CAST('2026-02-01 00:00:00' AS TIMESTAMP)),
  (CAST('2026-03-01 00:00:00' AS TIMESTAMP)) AS months_range(dt)
LEFT JOIN _s3 AS _s3
  ON _s3.sale_month = TRUNC(months_range.dt, 'MONTH')
ORDER BY
  TRUNC(months_range.dt, 'MONTH')
