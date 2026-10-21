WITH _s2 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    YEAR(CAST(_s0.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    1
), _s6 AS (
  SELECT
    YEAR(CAST(_s2.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    ARBITRARY(_s3.n_rows) AS anything_n_rows
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s3.year_ca_dt = YEAR(CAST(_s2.ca_dt AS TIMESTAMP))
  GROUP BY
    1
), _s7 AS (
  SELECT
    YEAR(CAST(_s4.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  _s6.year_ca_dt AS yr,
  ROUND(
    CAST(SUM(COALESCE(_s7.n_rows, 0)) OVER (ORDER BY _s6.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE) / SUM(COALESCE(_s6.anything_n_rows, 0)) OVER (ORDER BY _s6.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(_s6.anything_n_rows, 0) - LAG(COALESCE(_s6.anything_n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt)
      )
    ) AS DOUBLE) / LAG(COALESCE(_s6.anything_n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt),
    2
  ) AS pct_bought_change,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(_s7.n_rows, 0) - LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt)
      )
    ) AS DOUBLE) / LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt),
    2
  ) AS pct_incident_change,
  COALESCE(_s6.anything_n_rows, 0) AS bought,
  COALESCE(_s7.n_rows, 0) AS incidents
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year_ca_dt = _s7.year_ca_dt
ORDER BY
  1 NULLS FIRST
