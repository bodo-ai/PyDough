WITH _t4 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
  WHERE
    EXTRACT(YEAR FROM ca_dt) IN (2020, 2021)
), _t8 AS (
  SELECT
    co_id AS _id,
    co_name AS name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s7 AS (
  SELECT
    COUNT() AS agg_2,
    _t7.calendar_day
  FROM _t4 AS _t7
  JOIN main.incidents AS incidents
    ON _t7.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t8 AS _t8
    ON _t8._id = devices.de_production_country_id
  GROUP BY
    _t7.calendar_day
), _s15 AS (
  SELECT
    COUNT() AS agg_5,
    _t11.calendar_day
  FROM _t4 AS _t11
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATE_ADD(CAST(_t11.calendar_day AS TIMESTAMP), -6, 'MONTH')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t8 AS _t12
    ON _t12._id = devices.de_production_country_id
  GROUP BY
    _t11.calendar_day
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
