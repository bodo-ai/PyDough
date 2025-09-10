WITH _t2 AS (
  SELECT
    ca_dt
  FROM main.calendar
  WHERE
    EXTRACT(YEAR FROM CAST(ca_dt AS TIMESTAMP)) IN (2020, 2021)
), _t5 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    _t4.ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t4
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= CAST(_t4.ca_dt AS TIMESTAMP) - INTERVAL '6 MONTH'
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t5 AS _t5
    ON _t5.co_id = devices.de_production_country_id
  GROUP BY
    1
), _s15 AS (
  SELECT
    _t7.ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t7
  JOIN main.incidents AS incidents
    ON _t7.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t5 AS _t8
    ON _t8.co_id = devices.de_production_country_id
  GROUP BY
    1
), _t0 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(_t2.ca_dt AS TIMESTAMP)) AS month_ca_dt,
    EXTRACT(YEAR FROM CAST(_t2.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    SUM(_s7.n_rows) AS sum_expr_3,
    SUM(_s15.n_rows) AS sum_n_rows
  FROM _t2 AS _t2
  LEFT JOIN _s7 AS _s7
    ON _s7.ca_dt = _t2.ca_dt
  LEFT JOIN _s15 AS _s15
    ON _s15.ca_dt = _t2.ca_dt
  GROUP BY
    1,
    2
)
SELECT
  CONCAT_WS('-', year_ca_dt, LPAD(CAST(month_ca_dt AS TEXT), 2, '0')) AS month,
  ROUND(
    CAST((
      1000000.0 * COALESCE(sum_n_rows, 0)
    ) / COALESCE(sum_expr_3, 0) AS DECIMAL),
    2
  ) AS ir
FROM _t0
ORDER BY
  month_ca_dt NULLS FIRST
