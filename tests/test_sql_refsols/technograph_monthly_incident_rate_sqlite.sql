WITH _t2 AS (
  SELECT
    ca_dt
  FROM main.calendar
  WHERE
    CAST(STRFTIME('%Y', ca_dt) AS INTEGER) IN (2020, 2021)
), _s6 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%m', ca_dt) AS INTEGER) AS month_ca_dt,
    CAST(STRFTIME('%Y', ca_dt) AS INTEGER) AS year_ca_dt
  FROM _t2
), _t5 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    CAST(STRFTIME('%m', _t4.ca_dt) AS INTEGER) AS month_ca_dt,
    CAST(STRFTIME('%Y', _t4.ca_dt) AS INTEGER) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t4
  JOIN main.incidents AS incidents
    ON _t4.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t5 AS _t5
    ON _t5.co_id = devices.de_production_country_id
  GROUP BY
    1,
    2
), _s15 AS (
  SELECT
    CAST(STRFTIME('%m', _t7.ca_dt) AS INTEGER) AS month_ca_dt,
    CAST(STRFTIME('%Y', _t7.ca_dt) AS INTEGER) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t7
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATETIME(_t7.ca_dt, '-6 month')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t5 AS _t8
    ON _t8.co_id = devices.de_production_country_id
  GROUP BY
    1,
    2
)
SELECT
  CONCAT_WS(
    '-',
    _s6.year_ca_dt,
    CASE
      WHEN LENGTH(_s6.month_ca_dt) >= 2
      THEN SUBSTRING(_s6.month_ca_dt, 1, 2)
      ELSE SUBSTRING('00' || _s6.month_ca_dt, -2)
    END
  ) AS month,
  ROUND(
    CAST((
      1000000.0 * COALESCE(_s7.n_rows, 0)
    ) AS REAL) / COALESCE(_s15.n_rows, 0),
    2
  ) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month_ca_dt = _s7.month_ca_dt AND _s6.year_ca_dt = _s7.year_ca_dt
LEFT JOIN _s15 AS _s15
  ON _s15.month_ca_dt = _s6.month_ca_dt AND _s15.year_ca_dt = _s6.year_ca_dt
ORDER BY
  _s6.month_ca_dt
