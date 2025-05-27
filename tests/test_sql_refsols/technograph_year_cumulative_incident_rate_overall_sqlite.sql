WITH _t5 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s2 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%Y', calendar_day) AS INTEGER) AS year
  FROM _t5
), _s3 AS (
  SELECT
    COUNT() AS agg_0,
    CAST(STRFTIME('%Y', _t7.calendar_day) AS INTEGER) AS year
  FROM _t5 AS _t7
  JOIN main.devices AS devices
    ON _t7.calendar_day = DATE(devices.de_purchase_ts, 'start of day')
  GROUP BY
    CAST(STRFTIME('%Y', _t7.calendar_day) AS INTEGER)
), _s7 AS (
  SELECT
    COUNT() AS agg_1,
    CAST(STRFTIME('%Y', _t9.calendar_day) AS INTEGER) AS year
  FROM _t5 AS _t9
  JOIN main.incidents AS incidents
    ON _t9.calendar_day = DATE(incidents.in_error_report_ts, 'start of day')
  GROUP BY
    CAST(STRFTIME('%Y', _t9.calendar_day) AS INTEGER)
), _t0 AS (
  SELECT
    COALESCE(_s3.agg_0, 0) AS bought,
    ROUND(
      CAST(SUM(COALESCE(_s7.agg_1, 0)) OVER (ORDER BY _s2.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(_s3.agg_0, 0)) OVER (ORDER BY _s2.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    COALESCE(_s7.agg_1, 0) AS incidents,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(_s3.agg_0, 0) - LAG(COALESCE(_s3.agg_0, 0), 1) OVER (ORDER BY _s2.year)
        )
      ) AS REAL) / LAG(COALESCE(_s3.agg_0, 0), 1) OVER (ORDER BY _s2.year),
      2
    ) AS pct_bought_change,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(_s7.agg_1, 0) - LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s2.year)
        )
      ) AS REAL) / LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s2.year),
      2
    ) AS pct_incident_change,
    _s2.year AS yr
  FROM _s2 AS _s2
  LEFT JOIN _s3 AS _s3
    ON _s2.year = _s3.year
  LEFT JOIN _s7 AS _s7
    ON _s2.year = _s7.year
  WHERE
    NOT _s3.agg_0 IS NULL AND _s3.agg_0 > 0
)
SELECT
  yr,
  cum_ir,
  pct_bought_change,
  pct_incident_change,
  bought,
  incidents
FROM _t0
ORDER BY
  yr
