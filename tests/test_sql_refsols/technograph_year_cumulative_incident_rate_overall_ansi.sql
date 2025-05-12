WITH _t5 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s2 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM calendar_day) AS year
  FROM _t5
), _s3 AS (
  SELECT
    COUNT() AS agg_0,
    EXTRACT(YEAR FROM _t7.calendar_day) AS year
  FROM _t5 AS _t7
  JOIN main.devices AS devices
    ON _t7.calendar_day = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    EXTRACT(YEAR FROM _t7.calendar_day)
), _s7 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(YEAR FROM _t9.calendar_day) AS year
  FROM _t5 AS _t9
  JOIN main.incidents AS incidents
    ON _t9.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    EXTRACT(YEAR FROM _t9.calendar_day)
), _t0 AS (
  SELECT
    COALESCE(_s3.agg_0, 0) AS bought,
    COALESCE(_s7.agg_1, 0) AS incidents,
    _s2.year AS yr,
    ROUND(
      (
        100.0 * (
          COALESCE(_s3.agg_0, 0) - LAG(COALESCE(_s3.agg_0, 0), 1) OVER (ORDER BY _s2.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s3.agg_0, 0), 1) OVER (ORDER BY _s2.year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(_s7.agg_1, 0) - LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s2.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s2.year NULLS LAST),
      2
    ) AS pct_incident_change,
    ROUND(
      SUM(COALESCE(_s7.agg_1, 0)) OVER (ORDER BY _s2.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s3.agg_0, 0)) OVER (ORDER BY _s2.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir
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
