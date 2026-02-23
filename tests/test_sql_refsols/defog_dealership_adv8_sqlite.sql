WITH _s6 AS (
  SELECT DISTINCT
    months_range.column1 AS month_start
  FROM (VALUES
    ('2025-08-01 00:00:00'),
    ('2025-09-01 00:00:00'),
    ('2025-10-01 00:00:00'),
    ('2025-11-01 00:00:00'),
    ('2025-12-01 00:00:00'),
    ('2026-01-01 00:00:00')) AS months_range
  CROSS JOIN main.sales AS sales
), _s7 AS (
  SELECT
    months_range_2.column1 AS month_start,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM (VALUES
    ('2025-08-01 00:00:00'),
    ('2025-09-01 00:00:00'),
    ('2025-10-01 00:00:00'),
    ('2025-11-01 00:00:00'),
    ('2025-12-01 00:00:00'),
    ('2026-01-01 00:00:00')) AS months_range_2
  JOIN main.sales AS sales
    ON STRFTIME('%Y-%m', months_range_2.column1) = STRFTIME('%Y-%m', sales.sale_date)
  JOIN main.salespersons AS salespersons
    ON CAST(STRFTIME('%Y', salespersons.hire_date) AS INTEGER) <= 2023
    AND CAST(STRFTIME('%Y', salespersons.hire_date) AS INTEGER) >= 2022
    AND sales.salesperson_id = salespersons._id
  GROUP BY
    1
)
SELECT
  STRFTIME('%Y-%m-%d', _s6.month_start) AS month,
  COALESCE(_s7.n_rows, 0) AS PMSPS,
  COALESCE(_s7.sum_sale_price, 0) AS PMSR
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month_start = _s7.month_start
ORDER BY
  _s6.month_start
