WITH _s12 AS (
  SELECT
    COUNT(*) AS agg_2,
    _s1.ca_dt AS calendar_day
  FROM main.calendar AS _s1
  JOIN main.calendar AS _s2
    ON _s2.ca_dt >= DATE_ADD(CAST(_s1.ca_dt AS TIMESTAMP), -6, 'MONTH')
  JOIN main.devices AS _s5
    ON _s2.ca_dt = DATE_TRUNC('DAY', CAST(_s5.de_purchase_ts AS TIMESTAMP))
  JOIN main.countries AS _s8
    ON _s5.de_production_country_id = _s8.co_id AND _s8.co_name = 'CN'
  WHERE
    EXTRACT(YEAR FROM _s1.ca_dt) IN (2020, 2021)
  GROUP BY
    _s1.ca_dt
), _s24 AS (
  SELECT
    COUNT(*) AS agg_5,
    _s13.ca_dt AS calendar_day
  FROM main.calendar AS _s13
  JOIN main.incidents AS _s14
    ON _s13.ca_dt = DATE_TRUNC('DAY', CAST(_s14.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS _s17
    ON _s14.in_device_id = _s17.de_id
  JOIN main.countries AS _s18
    ON _s17.de_production_country_id = _s18.co_id AND _s18.co_name = 'CN'
  WHERE
    EXTRACT(YEAR FROM _s13.ca_dt) IN (2020, 2021)
  GROUP BY
    _s13.ca_dt
), _t1 AS (
  SELECT
    SUM(_s12.agg_2) AS agg_4,
    SUM(_s24.agg_5) AS agg_7,
    EXTRACT(MONTH FROM _s0.ca_dt) AS month,
    EXTRACT(YEAR FROM _s0.ca_dt) AS year
  FROM main.calendar AS _s0
  LEFT JOIN _s12 AS _s12
    ON _s0.ca_dt = _s12.calendar_day
  LEFT JOIN _s24 AS _s24
    ON _s0.ca_dt = _s24.calendar_day
  WHERE
    EXTRACT(YEAR FROM _s0.ca_dt) IN (2020, 2021)
  GROUP BY
    EXTRACT(MONTH FROM _s0.ca_dt),
    EXTRACT(YEAR FROM _s0.ca_dt)
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
