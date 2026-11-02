WITH _t1 AS (
  SELECT
    pr_name
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _t2 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _t4 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
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
  CROSS JOIN _t2 AS _t5
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
), _s15 AS (
  SELECT DISTINCT
    DATE(
      _s13.ca_dt,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_s13.ca_dt)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter
  FROM _t4 AS _t10
  CROSS JOIN _t2 AS _t11
  JOIN _s5 AS _s13
    ON _s13.ca_dt < DATE(
      DATETIME(_t10.pr_release, '2 year'),
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_t10.pr_release, '2 year')) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
    AND _s13.ca_dt >= _t10.pr_release
), _s21 AS (
  SELECT
    _s19.ca_dt
  FROM _t4 AS _t12
  CROSS JOIN _t2 AS _t13
  JOIN _s5 AS _s19
    ON _s19.ca_dt < DATE(
      DATETIME(_t12.pr_release, '2 year'),
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_t12.pr_release, '2 year')) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
    AND _s19.ca_dt >= _t12.pr_release
), _s27 AS (
  SELECT
    _s15.quarter,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM _t1 AS _t7
  JOIN main.countries AS countries
    ON countries.co_name = 'CN'
  CROSS JOIN _s15 AS _s15
  JOIN _s21 AS _s21
    ON _s15.quarter = DATE(
      _s21.ca_dt,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_s21.ca_dt)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
  JOIN main.incidents AS incidents
    ON _s21.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
    AND countries.co_id = incidents.in_repair_country_id
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id AND devices.de_product_id = 800544
  GROUP BY
    1
), _s35 AS (
  SELECT
    DATE(
      _s31.ca_dt,
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_s31.ca_dt)) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    ) AS quarter,
    COUNT(*) AS n_rows
  FROM _t4 AS _t16
  CROSS JOIN _t2 AS _t17
  JOIN _s5 AS _s31
    ON _s31.ca_dt < DATE(
      DATETIME(_t16.pr_release, '2 year'),
      'start of month',
      '-' || CAST((
        (
          CAST(STRFTIME('%m', DATETIME(_t16.pr_release, '2 year')) AS INTEGER) - 1
        ) % 3
      ) AS TEXT) || ' months'
    )
    AND _s31.ca_dt >= _t16.pr_release
  JOIN main.devices AS devices
    ON _s31.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
    AND devices.de_product_id = 800544
  GROUP BY
    1
)
SELECT
  _s7.quarter,
  COALESCE(_s27.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s35.n_rows, 0) AS n_sold,
  ROUND(
    CAST(SUM(COALESCE(_s27.ndistinct_in_device_id, 0)) OVER (ORDER BY _s7.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(_s35.n_rows, 0)) OVER (ORDER BY _s7.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM _t1 AS _t1
CROSS JOIN _t2 AS _t2
CROSS JOIN _s7 AS _s7
LEFT JOIN _s27 AS _s27
  ON _s27.quarter = _s7.quarter
LEFT JOIN _s35 AS _s35
  ON _s35.quarter = _s7.quarter
ORDER BY
  1
