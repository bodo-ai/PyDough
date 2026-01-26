WITH _s0 AS (
  SELECT
    column1 AS month_start
  FROM (VALUES
    (CAST('2025-07-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-08-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-09-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-10-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-11-01 00:00:00' AS TIMESTAMP)),
    (CAST('2025-12-01 00:00:00' AS TIMESTAMP))) AS months_range(month_start)
), _s6 AS (
  SELECT DISTINCT
    _s0.month_start
  FROM _s0 AS _s0
  CROSS JOIN main.sales AS sales
), _s7 AS (
  SELECT
    _s2.month_start,
    COUNT(*) AS n_rows,
    SUM(sales.sale_price) AS sum_sale_price
  FROM _s0 AS _s2
  JOIN main.sales AS sales
    ON TIME_TO_STR(_s2.month_start, '%Y-%m') = TIME_TO_STR(sales.sale_date, '%Y-%m')
  JOIN main.salespersons AS salespersons
    ON EXTRACT(YEAR FROM CAST(salespersons.hire_date AS DATETIME)) <= 2023
    AND EXTRACT(YEAR FROM CAST(salespersons.hire_date AS DATETIME)) >= 2022
    AND sales.salesperson_id = salespersons._id
  GROUP BY
    1
)
SELECT
  TIME_TO_STR(_s6.month_start, '%Y-%m-%d') AS month,
  COALESCE(_s7.n_rows, 0) AS PMSPS,
  COALESCE(_s7.sum_sale_price, 0) AS PMSR
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month_start = _s7.month_start
ORDER BY
  _s6.month_start
