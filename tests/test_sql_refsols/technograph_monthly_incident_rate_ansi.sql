WITH _t4 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _t6 AS (
  SELECT
    co_id AS _id,
    co_name AS name
  FROM main.countries
), _s7 AS (
  SELECT
    COUNT() AS agg_2,
    _s0.calendar_day
  FROM _t4 AS _s0
  JOIN main.incidents AS incidents
    ON _s0.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t6 AS _t6
    ON _t6._id = devices.de_production_country_id AND _t6.name = 'CN'
  GROUP BY
    _s0.calendar_day
), _s15 AS (
  SELECT
    COUNT() AS agg_5,
    _t9.calendar_day
  FROM _t4 AS _t9
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATE_ADD(CAST(_t9.calendar_day AS TIMESTAMP), -6, 'MONTH')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t6 AS _t10
    ON _t10._id = devices.de_production_country_id AND _t10.name = 'CN'
  WHERE
    EXTRACT(YEAR FROM _t9.calendar_day) IN (2020, 2021)
  GROUP BY
    _t9.calendar_day
), _t1 AS (
  SELECT
    SUM(_s7.agg_2) AS agg_4,
    SUM(_s15.agg_5) AS agg_7,
    EXTRACT(MONTH FROM _t4.calendar_day) AS month,
    EXTRACT(YEAR FROM _t4.calendar_day) AS year
  FROM _t4 AS _t4
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = _t4.calendar_day
  LEFT JOIN _s15 AS _s15
    ON _s15.calendar_day = _t4.calendar_day
  WHERE
    EXTRACT(YEAR FROM _t4.calendar_day) IN (2020, 2021)
  GROUP BY
    EXTRACT(MONTH FROM _t4.calendar_day),
    EXTRACT(YEAR FROM _t4.calendar_day)
)
SELECT
  CONCAT_WS(
    '-',
    year,
    CASE
      WHEN LENGTH(month) >= 2
      THEN SUBSTRING(month, 1, 2)
      ELSE SUBSTRING(CONCAT('00', month), -2)
    END
  ) AS month,
  ROUND((
    1000000.0 * COALESCE(agg_4, 0)
  ) / COALESCE(agg_7, 0), 2) AS ir
FROM _t1
ORDER BY
  month
