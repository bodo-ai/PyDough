WITH _t2 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
  WHERE
    EXTRACT(YEAR FROM CAST(ca_dt AS DATETIME)) IN (2020, 2021)
), _s6 AS (
  SELECT DISTINCT
    EXTRACT(MONTH FROM CAST(ca_dt AS DATETIME)) AS month_ca_dt,
    EXTRACT(YEAR FROM CAST(ca_dt AS DATETIME)) AS year_ca_dt
  FROM _t2
), _t5 AS (
  SELECT
    co_id,
    co_name
  FROM main.COUNTRIES
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(_t4.ca_dt AS DATETIME)) AS month_ca_dt,
    EXTRACT(YEAR FROM CAST(_t4.ca_dt AS DATETIME)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t4
  JOIN main.INCIDENTS AS INCIDENTS
    ON _t4.ca_dt = CAST(CAST(INCIDENTS.in_error_report_ts AS DATETIME) AS DATE)
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id
  JOIN _t5 AS _t5
    ON DEVICES.de_production_country_id = _t5.co_id
  GROUP BY
    1,
    2
), _s15 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(_t7.ca_dt AS DATETIME)) AS month_ca_dt,
    EXTRACT(YEAR FROM CAST(_t7.ca_dt AS DATETIME)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t7
  JOIN main.CALENDAR AS CALENDAR
    ON CALENDAR.ca_dt >= DATE_SUB(CAST(_t7.ca_dt AS DATETIME), INTERVAL '6' MONTH)
  JOIN main.DEVICES AS DEVICES
    ON CALENDAR.ca_dt = CAST(CAST(DEVICES.de_purchase_ts AS DATETIME) AS DATE)
  JOIN _t5 AS _t8
    ON DEVICES.de_production_country_id = _t8.co_id
  GROUP BY
    1,
    2
)
SELECT
  CONCAT_WS('-', _s6.year_ca_dt, LPAD(_s6.month_ca_dt, 2, '0')) AS month,
  ROUND((
    1000000.0 * COALESCE(_s7.n_rows, 0)
  ) / COALESCE(_s15.n_rows, 0), 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month_ca_dt = _s7.month_ca_dt AND _s6.year_ca_dt = _s7.year_ca_dt
LEFT JOIN _s15 AS _s15
  ON _s15.month_ca_dt = _s6.month_ca_dt AND _s15.year_ca_dt = _s6.year_ca_dt
ORDER BY
  _s6.month_ca_dt
