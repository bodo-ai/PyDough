WITH _t6 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s0.ca_dt
  FROM _t6 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  GROUP BY
    _s0.ca_dt
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s4.ca_dt
  FROM _t6 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  GROUP BY
    _s4.ca_dt
), _t4 AS (
  SELECT
    SUM(_s3.n_rows) AS sum_expr_3,
    SUM(_s7.n_rows) AS sum_n_rows,
    CAST(STRFTIME('%Y', _t6.ca_dt) AS INTEGER) AS year
  FROM _t6 AS _t6
  LEFT JOIN _s3 AS _s3
    ON _s3.ca_dt = _t6.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s7.ca_dt = _t6.ca_dt
  GROUP BY
    CAST(STRFTIME('%Y', _t6.ca_dt) AS INTEGER)
), _t0 AS (
  SELECT
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
    COALESCE(sum_expr_3, 0) AS n_devices,
    COALESCE(sum_n_rows, 0) AS n_incidents,
    year
  FROM _t4
  WHERE
    NOT sum_expr_3 IS NULL AND sum_expr_3 > 0
)
SELECT
  year AS yr,
  cum_ir,
  pct_bought_change,
  pct_incident_change,
  n_devices AS bought,
  n_incidents AS incidents
FROM _t0
ORDER BY
  year
