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
    ON _s0.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    2
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s4.ca_dt
  FROM _s2 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    2
), _t1 AS (
  SELECT
    SUM(_s3.n_rows) AS sum_expr_3,
    SUM(_s7.n_rows) AS sum_n_rows,
    YEAR(CAST(_s2.ca_dt AS TIMESTAMP)) AS year
  FROM _s2 AS _s2
  LEFT JOIN _s3 AS _s3
    ON _s2.ca_dt = _s3.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s2.ca_dt = _s7.ca_dt
  GROUP BY
    3
)
SELECT
  year AS yr,
  ROUND(
    SUM(COALESCE(sum_n_rows, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(sum_expr_3, 0)) OVER (ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    (
      100.0 * (
        COALESCE(sum_expr_3, 0) - LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY year)
      )
    ) / LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY year),
    2
  ) AS pct_bought_change,
  ROUND(
    (
      100.0 * (
        COALESCE(sum_n_rows, 0) - LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY year)
      )
    ) / LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY year),
    2
  ) AS pct_incident_change,
  COALESCE(sum_expr_3, 0) AS bought,
  COALESCE(sum_n_rows, 0) AS incidents
FROM _t1
WHERE
  NOT sum_expr_3 IS NULL AND sum_expr_3 > 0
ORDER BY
  1 NULLS FIRST
