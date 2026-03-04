WITH _s6 AS (
  SELECT DISTINCT
    months_range.month_start
  FROM (VALUES
    ROW(CAST('2025-09-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-10-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-11-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-12-01 00:00:00' AS DATETIME)),
    ROW(CAST('2026-01-01 00:00:00' AS DATETIME)),
    ROW(CAST('2026-02-01 00:00:00' AS DATETIME))) AS months_range(month_start)
  CROSS JOIN dealership.sales AS sales
), _s7 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(sales.sale_date AS DATETIME)),
        ' ',
        MONTH(CAST(sales.sale_date AS DATETIME)),
        ' 1'
      ),
      '%Y %c %e'
    ) AS sale_month,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM (VALUES
    ROW(CAST('2025-09-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-10-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-11-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-12-01 00:00:00' AS DATETIME)),
    ROW(CAST('2026-01-01 00:00:00' AS DATETIME)),
    ROW(CAST('2026-02-01 00:00:00' AS DATETIME))) AS months_range_2(month_start)
  JOIN dealership.sales AS sales
    ON DATE_FORMAT(months_range_2.month_start, '%Y-%m') = DATE_FORMAT(sales.sale_date, '%Y-%m')
  JOIN dealership.salespersons AS salespersons
    ON EXTRACT(YEAR FROM CAST(salespersons.hire_date AS DATETIME)) <= 2023
    AND EXTRACT(YEAR FROM CAST(salespersons.hire_date AS DATETIME)) >= 2022
    AND sales.salesperson_id = salespersons._id
  GROUP BY
    1
)
SELECT
  DATE_FORMAT(
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(months_range.dt AS DATETIME)),
        ' ',
        MONTH(CAST(months_range.dt AS DATETIME)),
        ' 1'
      ),
      '%Y %c %e'
    ),
    '%Y-%m-%d'
  ) AS month,
  COALESCE(_s3.n_rows, 0) AS PMSPS,
  COALESCE(_s3.sum_sale_price, 0) AS PMSR
FROM (VALUES
  ROW(CAST('2025-09-01 00:00:00' AS DATETIME)),
  ROW(CAST('2025-10-01 00:00:00' AS DATETIME)),
  ROW(CAST('2025-11-01 00:00:00' AS DATETIME)),
  ROW(CAST('2025-12-01 00:00:00' AS DATETIME)),
  ROW(CAST('2026-01-01 00:00:00' AS DATETIME)),
  ROW(CAST('2026-02-01 00:00:00' AS DATETIME))) AS months_range(dt)
LEFT JOIN _s3 AS _s3
  ON _s3.sale_month = STR_TO_DATE(
    CONCAT(
      YEAR(CAST(months_range.dt AS DATETIME)),
      ' ',
      MONTH(CAST(months_range.dt AS DATETIME)),
      ' 1'
    ),
    '%Y %c %e'
  )
ORDER BY
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(months_range.dt AS DATETIME)),
      ' ',
      MONTH(CAST(months_range.dt AS DATETIME)),
      ' 1'
    ),
    '%Y %c %e'
  )
