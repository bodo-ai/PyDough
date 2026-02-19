WITH _s6 AS (
  SELECT DISTINCT
    months_range.month_start
  FROM (VALUES
    ROW(CAST('2025-08-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-09-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-10-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-11-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-12-01 00:00:00' AS DATETIME)),
    ROW(CAST('2026-01-01 00:00:00' AS DATETIME))) AS months_range(month_start)
  CROSS JOIN dealership.sales AS sales
), _s7 AS (
  SELECT
    months_range_2.month_start,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM (VALUES
    ROW(CAST('2025-08-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-09-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-10-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-11-01 00:00:00' AS DATETIME)),
    ROW(CAST('2025-12-01 00:00:00' AS DATETIME)),
    ROW(CAST('2026-01-01 00:00:00' AS DATETIME))) AS months_range_2(month_start)
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
  DATE_FORMAT(_s6.month_start, '%Y-%m-%d') AS month,
  COALESCE(_s7.n_rows, 0) AS PMSPS,
  COALESCE(_s7.sum_sale_price, 0) AS PMSR
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month_start = _s7.month_start
ORDER BY
  _s6.month_start
