WITH _t4 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _t8 AS (
  SELECT
    co_id AS _id,
    co_name AS name
  FROM main.countries
), _s7 AS (
  SELECT
    COUNT() AS agg_2,
    _t7.calendar_day
  FROM _t4 AS _t7
  JOIN main.calendar AS calendar
    ON calendar.ca_dt >= DATETIME(_t7.calendar_day, '-6 month')
  JOIN main.devices AS devices
    ON calendar.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t8 AS _t8
    ON _t8._id = devices.de_production_country_id AND _t8.name = 'CN'
  WHERE
    CAST(STRFTIME('%Y', _t7.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    _t7.calendar_day
), _s15 AS (
  SELECT
    COUNT() AS agg_5,
    _t11.calendar_day
  FROM _t4 AS _t11
  JOIN main.incidents AS incidents
    ON _t11.calendar_day = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t8 AS _t12
    ON _t12._id = devices.de_production_country_id AND _t12.name = 'CN'
  WHERE
    CAST(STRFTIME('%Y', _t11.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    _t11.calendar_day
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
