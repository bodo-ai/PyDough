WITH _s0 AS (
  SELECT
    months_range.month_start
  FROM (VALUES
    (CAST('2025-08-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-09-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-10-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-11-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-12-01 00:00:00' AS TIMESTAMP)),
    (CAST('2026-01-01 00:00:00' AS TIMESTAMP))) AS months_range(month_start)
), _s6 AS (
  SELECT DISTINCT
    _s0.month_start
  FROM _s0 AS _s0
  CROSS JOIN dealership.sales AS sales
), _s7 AS (
  SELECT
    _s2.month_start,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM _s0 AS _s2
  JOIN dealership.sales AS sales
    ON TO_CHAR(CAST(sales.sale_date AS TIMESTAMP), 'yyyy-mm') = TO_CHAR(_s2.month_start, 'yyyy-mm')
  JOIN dealership.salespersons AS salespersons
    ON YEAR(CAST(salespersons.hire_date AS TIMESTAMP)) <= 2023
    AND YEAR(CAST(salespersons.hire_date AS TIMESTAMP)) >= 2022
    AND sales.salesperson_id = salespersons.id
  GROUP BY
    1
)
SELECT
  TO_CHAR(_s6.month_start, 'yyyy-mm-DD') AS month,
  COALESCE(_s7.n_rows, 0) AS PMSPS,
  COALESCE(_s7.sum_sale_price, 0) AS PMSR
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month_start = _s7.month_start
ORDER BY
  _s6.month_start NULLS FIRST
