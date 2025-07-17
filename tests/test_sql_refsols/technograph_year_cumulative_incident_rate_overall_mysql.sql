WITH _t5 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s0.ca_dt
  FROM _t5 AS _s0
  JOIN main.DEVICES AS DEVICES
    ON _s0.ca_dt = DATE(CAST(DEVICES.de_purchase_ts AS DATETIME))
  GROUP BY
    _s0.ca_dt
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s4.ca_dt
  FROM _t5 AS _s4
  JOIN main.INCIDENTS AS INCIDENTS
    ON _s4.ca_dt = DATE(CAST(INCIDENTS.in_error_report_ts AS DATETIME))
  GROUP BY
    _s4.ca_dt
), _t3 AS (
  SELECT
    SUM(_s3.n_rows) AS sum_expr_3,
    SUM(_s7.n_rows) AS sum_n_rows,
    YEAR(_t5.ca_dt) AS year
  FROM _t5 AS _t5
  LEFT JOIN _s3 AS _s3
    ON _s3.ca_dt = _t5.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s7.ca_dt = _t5.ca_dt
  GROUP BY
    YEAR(_t5.ca_dt)
), _t0 AS (
  SELECT
    ROUND(
      SUM(COALESCE(sum_n_rows, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(sum_expr_3, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    ROUND(
      (
        100.0 * (
          COALESCE(sum_expr_3, 0) - LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY CASE WHEN year IS NULL THEN 1 ELSE 0 END, year)
        )
      ) / LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY CASE WHEN year IS NULL THEN 1 ELSE 0 END, year),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(sum_n_rows, 0) - LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY CASE WHEN year IS NULL THEN 1 ELSE 0 END, year)
        )
      ) / LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY CASE WHEN year IS NULL THEN 1 ELSE 0 END, year),
      2
    ) AS pct_incident_change,
    COALESCE(sum_expr_3, 0) AS n_devices,
    COALESCE(sum_n_rows, 0) AS n_incidents,
    year
  FROM _t3
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
