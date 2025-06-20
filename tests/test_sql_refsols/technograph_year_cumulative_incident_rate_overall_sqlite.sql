WITH _s6 AS (
  SELECT
    COUNT(*) AS agg_2,
    _s1.ca_dt AS calendar_day
  FROM main.calendar AS _s1
  JOIN main.devices AS _s2
    ON _s1.ca_dt = DATE(_s2.de_purchase_ts, 'start of day')
  GROUP BY
    _s1.ca_dt
), _s12 AS (
  SELECT
    COUNT(*) AS agg_5,
    _s7.ca_dt AS calendar_day
  FROM main.calendar AS _s7
  JOIN main.incidents AS _s8
    ON _s7.ca_dt = DATE(_s8.in_error_report_ts, 'start of day')
  GROUP BY
    _s7.ca_dt
), _t3 AS (
  SELECT
    SUM(_s6.agg_2) AS agg_4,
    SUM(_s12.agg_5) AS agg_7,
    CAST(STRFTIME('%Y', _s0.ca_dt) AS INTEGER) AS year
  FROM main.calendar AS _s0
  LEFT JOIN _s6 AS _s6
    ON _s0.ca_dt = _s6.calendar_day
  LEFT JOIN _s12 AS _s12
    ON _s0.ca_dt = _s12.calendar_day
  GROUP BY
    CAST(STRFTIME('%Y', _s0.ca_dt) AS INTEGER)
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
    year AS yr
  FROM _t3
  WHERE
    NOT agg_4 IS NULL AND agg_4 > 0
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
