WITH _t4 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _t5 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s5 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s7 AS (
  SELECT DISTINCT
    DATE(
      _s5.ca_dt,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_s5.ca_dt)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter
  FROM _t4 AS _t4
  CROSS JOIN _t5 AS _t5
  JOIN _s5 AS _s5
    ON _s5.ca_dt < DATE(
      DATETIME(_t4.pr_release, '2 year'),
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_t4.pr_release, '2 year')) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
    AND _s5.ca_dt >= _t4.pr_release
), _s17 AS (
  SELECT
    DATE(
      _s11.ca_dt,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_s11.ca_dt)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter,
    incidents.in_repair_country_id,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM _t4 AS _t7
  CROSS JOIN _t5 AS _t8
  JOIN _s5 AS _s11
    ON _s11.ca_dt < DATE(
      DATETIME(_t7.pr_release, '2 year'),
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_t7.pr_release, '2 year')) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
    AND _s11.ca_dt >= _t7.pr_release
  JOIN main.incidents AS incidents
    ON _s11.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id AND devices.de_product_id = 800544
  GROUP BY
    1,
    2
), _s25 AS (
  SELECT
    DATE(
      _s21.ca_dt,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_s21.ca_dt)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter,
    COUNT(*) AS n_rows
  FROM _t4 AS _t11
  CROSS JOIN _t5 AS _t12
  JOIN _s5 AS _s21
    ON _s21.ca_dt < DATE(
      DATETIME(_t11.pr_release, '2 year'),
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_t11.pr_release, '2 year')) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
    AND _s21.ca_dt >= _t11.pr_release
  JOIN main.devices AS devices
    ON _s21.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
    AND devices.de_product_id = 800544
  GROUP BY
    1
)
SELECT
  _s7.quarter,
  COALESCE(_s17.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s25.n_rows, 0) AS n_sold,
  ROUND(
    CAST(SUM(COALESCE(_s17.ndistinct_in_device_id, 0)) OVER (ORDER BY _s7.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(_s25.n_rows, 0)) OVER (ORDER BY _s7.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM main.products AS products
JOIN main.countries AS countries
  ON countries.co_name = 'CN'
CROSS JOIN _s7 AS _s7
LEFT JOIN _s17 AS _s17
  ON _s17.in_repair_country_id = countries.co_id AND _s17.quarter = _s7.quarter
LEFT JOIN _s25 AS _s25
  ON _s25.quarter = _s7.quarter
WHERE
  products.pr_name = 'RubyCopper-Star'
ORDER BY
  1
