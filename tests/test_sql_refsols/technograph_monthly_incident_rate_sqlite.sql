WITH _t3 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s6 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%m', calendar_day) AS INTEGER) AS month,
    CAST(STRFTIME('%Y', calendar_day) AS INTEGER) AS year
  FROM _t3
  WHERE
    CAST(STRFTIME('%Y', calendar_day) AS INTEGER) IN (2020, 2021)
), _t7 AS (
  SELECT
    co_id AS _id,
    co_name AS name
  FROM main.countries
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    CAST(STRFTIME('%m', _t6.calendar_day) AS INTEGER) AS month,
    CAST(STRFTIME('%Y', _t6.calendar_day) AS INTEGER) AS year
  FROM _t3 AS _t6
  JOIN main.incidents AS incidents
    ON _t6.calendar_day = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t7 AS _t7
    ON _t7._id = devices.de_production_country_id AND _t7.name = 'CN'
  WHERE
    CAST(STRFTIME('%Y', _t6.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    CAST(STRFTIME('%m', _t6.calendar_day) AS INTEGER),
    CAST(STRFTIME('%Y', _t6.calendar_day) AS INTEGER)
), _s15 AS (
  SELECT
    COUNT() AS agg_1,
    CAST(STRFTIME('%m', _t10.calendar_day) AS INTEGER) AS month,
    CAST(STRFTIME('%Y', _t10.calendar_day) AS INTEGER) AS year
  FROM _t3 AS _t10
  JOIN _t3 AS _s9
    ON _s9.calendar_day >= DATETIME(_t10.calendar_day, '-6 month')
  JOIN main.devices AS devices
    ON _s9.calendar_day = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t7 AS _t11
    ON _t11._id = devices.de_production_country_id AND _t11.name = 'CN'
  WHERE
    CAST(STRFTIME('%Y', _t10.calendar_day) AS INTEGER) IN (2020, 2021)
  GROUP BY
    CAST(STRFTIME('%m', _t10.calendar_day) AS INTEGER),
    CAST(STRFTIME('%Y', _t10.calendar_day) AS INTEGER)
)
SELECT
  CONCAT_WS(
    '-',
    _s6.year,
    CASE
      WHEN LENGTH(_s6.month) >= 2
      THEN SUBSTRING(_s6.month, 1, 2)
      ELSE SUBSTRING('00' || _s6.month, -2)
    END
  ) AS month,
  ROUND(
    CAST((
      1000000.0 * COALESCE(_s7.agg_0, 0)
    ) AS REAL) / COALESCE(_s15.agg_1, 0),
    2
  ) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.month = _s7.month AND _s6.year = _s7.year
LEFT JOIN _s15 AS _s15
  ON _s15.month = _s6.month AND _s15.year = _s6.year
ORDER BY
  month
