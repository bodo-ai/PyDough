WITH _t2 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
  WHERE
    YEAR(ca_dt) IN (2020, 2021)
), _t5 AS (
  SELECT
    co_id,
    co_name
  FROM main.COUNTRIES
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t4.ca_dt
  FROM _t2 AS _t4
  JOIN main.CALENDAR AS CALENDAR
    ON CALENDAR.ca_dt >= DATE_ADD(CAST(_t4.ca_dt AS DATETIME), INTERVAL '-6' MONTH)
  JOIN main.DEVICES AS DEVICES
    ON CALENDAR.ca_dt = DATE(CAST(DEVICES.de_purchase_ts AS DATETIME))
  JOIN _t5 AS _t5
    ON DEVICES.de_production_country_id = _t5.co_id
  GROUP BY
    _t4.ca_dt
), _s15 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t7.ca_dt
  FROM _t2 AS _t7
  JOIN main.INCIDENTS AS INCIDENTS
    ON _t7.ca_dt = DATE(CAST(INCIDENTS.in_error_report_ts AS DATETIME))
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id
  JOIN _t5 AS _t8
    ON DEVICES.de_production_country_id = _t8.co_id
  GROUP BY
    _t7.ca_dt
)
SELECT
  CONCAT_WS('-', YEAR(_t2.ca_dt), LPAD(MONTH(_t2.ca_dt), 2, '0')) AS month,
  ROUND((
    1000000.0 * COALESCE(SUM(_s15.n_rows), 0)
  ) / COALESCE(SUM(_s7.n_rows), 0), 2) AS ir
FROM _t2 AS _t2
LEFT JOIN _s7 AS _s7
  ON _s7.ca_dt = _t2.ca_dt
LEFT JOIN _s15 AS _s15
  ON _s15.ca_dt = _t2.ca_dt
GROUP BY
  MONTH(_t2.ca_dt),
  YEAR(_t2.ca_dt)
ORDER BY
  month
