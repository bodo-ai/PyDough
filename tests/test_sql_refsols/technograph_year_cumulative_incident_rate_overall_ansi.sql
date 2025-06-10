WITH _s0 AS (
  SELECT
    ca_dt AS calendar_day,
    ca_dt AS key_0
  FROM main.calendar
), _s3 AS (
  SELECT
    COUNT() AS agg_2,
    _s0.calendar_day
  FROM _s0 AS _s0
  JOIN main.devices AS devices
    ON _s0.key_0 = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    _s0.calendar_day
), _s7 AS (
  SELECT
    COUNT() AS agg_5,
    _s4.calendar_day
  FROM _s0 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.key_0 = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    _s4.calendar_day
), _t3 AS (
  SELECT
    SUM(_s3.agg_2) AS agg_4,
    SUM(_s7.agg_5) AS agg_7,
    EXTRACT(YEAR FROM calendar.ca_dt) AS year
  FROM main.calendar AS calendar
  LEFT JOIN _s3 AS _s3
    ON _s3.calendar_day = calendar.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = calendar.ca_dt
  GROUP BY
    EXTRACT(YEAR FROM calendar.ca_dt)
), _t0 AS (
  SELECT
    COALESCE(agg_4, 0) AS bought,
    ROUND(
      SUM(COALESCE(agg_7, 0)) OVER (ORDER BY year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(agg_4, 0)) OVER (ORDER BY year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    COALESCE(agg_7, 0) AS incidents,
    ROUND(
      (
        100.0 * (
          COALESCE(agg_4, 0) - LAG(COALESCE(agg_4, 0), 1) OVER (ORDER BY year NULLS LAST)
        )
      ) / LAG(COALESCE(agg_4, 0), 1) OVER (ORDER BY year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(agg_7, 0) - LAG(COALESCE(agg_7, 0), 1) OVER (ORDER BY year NULLS LAST)
        )
      ) / LAG(COALESCE(agg_7, 0), 1) OVER (ORDER BY year NULLS LAST),
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
