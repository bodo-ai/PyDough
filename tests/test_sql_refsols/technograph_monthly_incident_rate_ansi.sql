WITH _t4 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _t8 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
), _s7 AS (
  SELECT
    COUNT() AS agg_2,
    _s1.ca_dt AS calendar_day
  FROM _t4 AS _t7
  JOIN _t4 AS _s1
    ON _s1.ca_dt >= DATE_ADD(CAST(_t7.ca_dt AS TIMESTAMP), -6, 'MONTH')
  JOIN main.devices AS devices
    ON _s1.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t8 AS _t8
    ON _t8.co_id = devices.de_production_country_id AND _t8.co_name = 'CN'
  WHERE
    EXTRACT(YEAR FROM _t7.ca_dt) IN (2020, 2021)
  GROUP BY
    _s1.ca_dt
), _s15 AS (
  SELECT
    COUNT() AS agg_5,
    _t11.ca_dt AS calendar_day
  FROM _t4 AS _t11
  JOIN main.incidents AS incidents
    ON _t11.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t8 AS _t12
    ON _t12.co_id = devices.de_production_country_id AND _t12.co_name = 'CN'
  WHERE
    EXTRACT(YEAR FROM _t11.ca_dt) IN (2020, 2021)
  GROUP BY
    _t11.ca_dt
), _t1 AS (
  SELECT
    SUM(_s7.agg_2) AS agg_4,
    SUM(_s15.agg_5) AS agg_7,
    EXTRACT(MONTH FROM _t4.ca_dt) AS month,
    EXTRACT(YEAR FROM _t4.ca_dt) AS year
  FROM _t4 AS _t4
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = _t4.ca_dt
  LEFT JOIN _s15 AS _s15
    ON _s15.calendar_day = _t4.ca_dt
  WHERE
    EXTRACT(YEAR FROM _t4.ca_dt) IN (2020, 2021)
  GROUP BY
    EXTRACT(MONTH FROM _t4.ca_dt),
    EXTRACT(YEAR FROM _t4.ca_dt)
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
    1000000.0 * COALESCE(agg_7, 0)
  ) / COALESCE(agg_4, 0), 2) AS ir
FROM _t1
ORDER BY
  month
