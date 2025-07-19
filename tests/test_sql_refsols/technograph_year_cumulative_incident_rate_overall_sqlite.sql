WITH _s2 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s0.ca_dt
  FROM _s2 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  GROUP BY
    _s0.ca_dt
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s4.ca_dt
  FROM _s2 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  GROUP BY
    _s4.ca_dt
), _t2 AS (
  SELECT
    SUM(_s3.n_rows) AS sum_expr_3,
    SUM(_s7.n_rows) AS sum_n_rows,
    CAST(STRFTIME('%Y', _s2.ca_dt) AS INTEGER) AS year
  FROM _s2 AS _s2
  LEFT JOIN _s3 AS _s3
    ON _s2.ca_dt = _s3.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s2.ca_dt = _s7.ca_dt
  GROUP BY
    CAST(STRFTIME('%Y', _s2.ca_dt) AS INTEGER)
)
SELECT
  year AS yr,
  ROUND(
    CAST(SUM(COALESCE(sum_n_rows, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(sum_expr_3, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(sum_expr_3, 0) - LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY year)
      )
    ) AS REAL) / LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY year),
    2
  ) AS pct_bought_change,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(sum_n_rows, 0) - LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY year)
      )
    ) AS REAL) / LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY year),
    2
  ) AS pct_incident_change,
  COALESCE(sum_expr_3, 0) AS bought,
  COALESCE(sum_n_rows, 0) AS incidents
FROM _t2
WHERE
  NOT sum_expr_3 IS NULL AND sum_expr_3 > 0
ORDER BY
  year
