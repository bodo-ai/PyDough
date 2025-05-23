WITH _t3 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
  WHERE
    EXTRACT(YEAR FROM ca_dt) IN (2020, 2021)
), _s6 AS (
  SELECT DISTINCT
    EXTRACT(MONTH FROM calendar_day) AS month,
    EXTRACT(YEAR FROM calendar_day) AS year
  FROM _t3
), _t7 AS (
  SELECT
    co_id AS _id,
    co_name AS name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    EXTRACT(MONTH FROM _t6.calendar_day) AS month,
    EXTRACT(YEAR FROM _t6.calendar_day) AS year
  FROM _t3 AS _t6
  JOIN main.incidents AS incidents
    ON _t6.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t7 AS _t7
    ON _t7._id = devices.de_production_country_id
  GROUP BY
    EXTRACT(MONTH FROM _t6.calendar_day),
    EXTRACT(YEAR FROM _t6.calendar_day)
), _s15 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(MONTH FROM _t10.calendar_day) AS month,
    EXTRACT(YEAR FROM _t10.calendar_day) AS year
  FROM _t3 AS _t10
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATE_ADD(CAST(_t10.calendar_day AS TIMESTAMP), -6, 'MONTH')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t7 AS _t11
    ON _t11._id = devices.de_production_country_id
  GROUP BY
    EXTRACT(MONTH FROM _t10.calendar_day),
    EXTRACT(YEAR FROM _t10.calendar_day)
)
SELECT
  CONCAT_WS(
    '-',
    _s6.year,
    CASE
      WHEN LENGTH(_s6.month) >= 2
      THEN SUBSTRING(_s6.month, 1, 2)
      ELSE SUBSTRING(CONCAT('00', _s6.month), -2)
    END
  ) AS month,
  ROUND((
    1000000.0 * COALESCE(_s7.agg_0, 0)
  ) / COALESCE(_s15.agg_1, 0), 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month = _s7.month AND _s6.year = _s7.year
LEFT JOIN _s15 AS _s15
  ON _s15.month = _s6.month AND _s15.year = _s6.year
ORDER BY
  month
