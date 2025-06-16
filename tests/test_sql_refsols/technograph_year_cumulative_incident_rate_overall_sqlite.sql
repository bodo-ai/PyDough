WITH _t5 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    COUNT() AS agg_2,
    _s0.ca_dt AS calendar_day
  FROM _t5 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  GROUP BY
    _s0.ca_dt
), _s7 AS (
  SELECT
    COUNT() AS agg_5,
    _s4.ca_dt AS calendar_day
  FROM _t5 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  GROUP BY
    _s4.ca_dt
), _t3 AS (
  SELECT
    SUM(_s3.agg_2) AS agg_4,
    SUM(_s7.agg_5) AS agg_7,
    CAST(STRFTIME('%Y', _t5.ca_dt) AS INTEGER) AS year
  FROM _t5 AS _t5
  LEFT JOIN _s3 AS _s3
    ON _s3.calendar_day = _t5.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = _t5.ca_dt
  GROUP BY
    CAST(STRFTIME('%Y', _t5.ca_dt) AS INTEGER)
), _t0 AS (
  SELECT
    COALESCE(agg_4, 0) AS bought,
    ROUND(
      CAST(SUM(COALESCE(agg_7, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(agg_4, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    COALESCE(agg_7, 0) AS incidents,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(agg_4, 0) - LAG(COALESCE(agg_4, 0), 1) OVER (ORDER BY year)
        )
      ) AS REAL) / LAG(COALESCE(agg_4, 0), 1) OVER (ORDER BY year),
      2
    ) AS pct_bought_change,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(agg_7, 0) - LAG(COALESCE(agg_7, 0), 1) OVER (ORDER BY year)
        )
      ) AS REAL) / LAG(COALESCE(agg_7, 0), 1) OVER (ORDER BY year),
      2
    ) AS pct_incident_change,
    year
  FROM _t3
  WHERE
    NOT agg_4 IS NULL AND agg_4 > 0
)
SELECT
  year AS yr,
  cum_ir,
  pct_bought_change,
  pct_incident_change,
  bought,
  incidents
FROM _t0
ORDER BY
  year
