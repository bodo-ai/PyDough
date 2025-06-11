WITH _t4 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s4 AS (
  SELECT
    COUNT() AS agg_2,
    calendar.ca_dt AS calendar_day,
    devices.de_production_country_id AS factory_country_id
  FROM _t4 AS _t8
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATETIME(_t8.calendar_day, '-6 month')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  WHERE
    CAST(STRFTIME('%Y', _t8.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    calendar.ca_dt,
    devices.de_production_country_id
), _t9 AS (
  SELECT
    co_id AS _id,
    co_name AS name
  FROM main.countries
), _s7 AS (
  SELECT
    SUM(_s4.agg_2) AS agg_2,
    _s4.calendar_day
  FROM _s4 AS _s4
  JOIN _t9 AS _t9
    ON _s4.factory_country_id = _t9._id AND _t9.name = 'CN'
  GROUP BY
    _s4.calendar_day
), _s12 AS (
  SELECT
    COUNT() AS agg_5,
    _t13.calendar_day,
    incidents.in_device_id AS device_id
  FROM _t4 AS _t13
  JOIN main.incidents AS incidents
    ON _t13.calendar_day = DATE(incidents.in_error_report_ts, 'start of day')
  WHERE
    CAST(STRFTIME('%Y', _t13.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    _t13.calendar_day,
    incidents.in_device_id
), _s10 AS (
  SELECT DISTINCT
    de_production_country_id AS factory_country_id
  FROM main.devices
), _s13 AS (
  SELECT
    _t15._id
  FROM _s10 AS _s10
  JOIN _t9 AS _t15
    ON _s10.factory_country_id = _t15._id AND _t15.name = 'CN'
), _s15 AS (
  SELECT
    SUM(_s12.agg_5) AS agg_5,
    _s12.calendar_day
  FROM _s12 AS _s12
  JOIN _s13 AS _s13
    ON _s12.device_id = _s13._id
  GROUP BY
    _s12.calendar_day
), _t1 AS (
  SELECT
    SUM(_s7.agg_2) AS agg_4,
    SUM(_s15.agg_5) AS agg_7,
    CAST(STRFTIME('%m', _t4.calendar_day) AS INTEGER) AS month,
    CAST(STRFTIME('%Y', _t4.calendar_day) AS INTEGER) AS year
  FROM _t4 AS _t4
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = _t4.calendar_day
  LEFT JOIN _s15 AS _s15
    ON _s15.calendar_day = _t4.calendar_day
  WHERE
    CAST(STRFTIME('%Y', _t4.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    CAST(STRFTIME('%m', _t4.calendar_day) AS INTEGER),
    CAST(STRFTIME('%Y', _t4.calendar_day) AS INTEGER)
)
SELECT
  CONCAT_WS(
    '-',
    year,
    CASE
      WHEN LENGTH(month) >= 2
      THEN SUBSTRING(month, 1, 2)
      ELSE SUBSTRING('00' || month, -2)
    END
  ) AS month,
  ROUND(CAST((
    1000000.0 * COALESCE(agg_7, 0)
  ) AS REAL) / COALESCE(agg_4, 0), 2) AS ir
FROM _t1
ORDER BY
  month
