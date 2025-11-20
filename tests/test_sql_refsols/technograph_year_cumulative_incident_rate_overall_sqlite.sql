WITH _s2 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    _s0.ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  GROUP BY
    1
), _s7 AS (
  SELECT
    _s4.ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  GROUP BY
    1
), _t1 AS (
  SELECT
    CAST(STRFTIME('%Y', _s2.ca_dt) AS INTEGER) AS year_cadt,
    SUM(_s3.n_rows) AS sum_expr3,
    SUM(_s7.n_rows) AS sum_nrows
  FROM _s2 AS _s2
  LEFT JOIN _s3 AS _s3
    ON _s2.ca_dt = _s3.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s2.ca_dt = _s7.ca_dt
  GROUP BY
    1
)
SELECT
  year_cadt AS yr,
  ROUND(
    CAST(SUM(COALESCE(sum_nrows, 0)) OVER (ORDER BY year_cadt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(sum_expr3) OVER (ORDER BY year_cadt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    CAST((
      100.0 * (
        sum_expr3 - LAG(sum_expr3, 1) OVER (ORDER BY year_cadt)
      )
    ) AS REAL) / LAG(sum_expr3, 1) OVER (ORDER BY year_cadt),
    2
  ) AS pct_bought_change,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(sum_nrows, 0) - LAG(COALESCE(sum_nrows, 0), 1) OVER (ORDER BY year_cadt)
      )
    ) AS REAL) / LAG(COALESCE(sum_nrows, 0), 1) OVER (ORDER BY year_cadt),
    2
  ) AS pct_incident_change,
  sum_expr3 AS bought,
  COALESCE(sum_nrows, 0) AS incidents
FROM _t1
WHERE
  NOT sum_expr3 IS NULL AND sum_expr3 > 0
ORDER BY
  1
