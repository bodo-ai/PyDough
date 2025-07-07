WITH _t4 AS (
  SELECT
    ca_dt
  FROM main.calendar
  WHERE
    CAST(STRFTIME('%Y', ca_dt) AS INTEGER) IN (2020, 2021)
), _t8 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t7.ca_dt
  FROM _t4 AS _t7
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATETIME(_t7.ca_dt, '-6 month')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t8 AS _t8
    ON _t8.co_id = devices.de_production_country_id
  GROUP BY
    _t7.ca_dt
), _s15 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t11.ca_dt
  FROM _t4 AS _t11
  JOIN main.incidents AS incidents
    ON _t11.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t8 AS _t12
    ON _t12.co_id = devices.de_production_country_id
  GROUP BY
    _t11.ca_dt
)
SELECT
  CONCAT_WS(
    '-',
    CAST(STRFTIME('%Y', _t4.ca_dt) AS INTEGER),
    CASE
      WHEN LENGTH(CAST(STRFTIME('%m', _t4.ca_dt) AS INTEGER)) >= 2
      THEN SUBSTRING(CAST(STRFTIME('%m', _t4.ca_dt) AS INTEGER), 1, 2)
      ELSE SUBSTRING('00' || CAST(STRFTIME('%m', _t4.ca_dt) AS INTEGER), (
        2 * -1
      ))
    END
  ) AS month,
  ROUND(
    CAST((
      1000000.0 * COALESCE(SUM(_s15.n_rows), 0)
    ) AS REAL) / COALESCE(SUM(_s7.n_rows), 0),
    2
  ) AS ir
FROM _t4 AS _t4
LEFT JOIN _s7 AS _s7
  ON _s7.ca_dt = _t4.ca_dt
LEFT JOIN _s15 AS _s15
  ON _s15.ca_dt = _t4.ca_dt
GROUP BY
  CAST(STRFTIME('%m', _t4.ca_dt) AS INTEGER),
  CAST(STRFTIME('%Y', _t4.ca_dt) AS INTEGER)
ORDER BY
  month
