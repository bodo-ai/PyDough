WITH _t3 AS (
  SELECT
    ca_dt
  FROM main.calendar
  WHERE
    EXTRACT(YEAR FROM CAST(ca_dt AS DATETIME)) IN (2020, 2021)
), _t7 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t6.ca_dt
  FROM _t3 AS _t6
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATE_ADD(CAST(_t6.ca_dt AS TIMESTAMP), -6, 'MONTH')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t7 AS _t7
    ON _t7.co_id = devices.de_production_country_id
  GROUP BY
    _t6.ca_dt
), _s15 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t10.ca_dt
  FROM _t3 AS _t10
  JOIN main.incidents AS incidents
    ON _t10.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t7 AS _t11
    ON _t11.co_id = devices.de_production_country_id
  GROUP BY
    _t10.ca_dt
)
SELECT
  CONCAT_WS(
    '-',
    EXTRACT(YEAR FROM CAST(_t3.ca_dt AS DATETIME)),
    CASE
      WHEN LENGTH(EXTRACT(MONTH FROM CAST(_t3.ca_dt AS DATETIME))) >= 2
      THEN SUBSTRING(EXTRACT(MONTH FROM CAST(_t3.ca_dt AS DATETIME)), 1, 2)
      ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM CAST(_t3.ca_dt AS DATETIME))), (
        2 * -1
      ))
    END
  ) AS month,
  ROUND((
    1000000.0 * COALESCE(SUM(_s15.n_rows), 0)
  ) / COALESCE(SUM(_s7.n_rows), 0), 2) AS ir
FROM _t3 AS _t3
LEFT JOIN _s7 AS _s7
  ON _s7.ca_dt = _t3.ca_dt
LEFT JOIN _s15 AS _s15
  ON _s15.ca_dt = _t3.ca_dt
GROUP BY
  EXTRACT(MONTH FROM CAST(_t3.ca_dt AS DATETIME)),
  EXTRACT(YEAR FROM CAST(_t3.ca_dt AS DATETIME))
ORDER BY
  month
