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
    ON _s0.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    1
), _s7 AS (
  SELECT
    _s4.ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    1
), _t1 AS (
  SELECT
    YEAR(CAST(_s2.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    SUM(_s3.n_rows) AS sum_expr_3,
    SUM(_s7.n_rows) AS sum_n_rows
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s2.ca_dt = _s3.ca_dt
  JOIN _s7 AS _s7
    ON _s2.ca_dt = _s7.ca_dt
  GROUP BY
    1
)
SELECT
  year_ca_dt AS yr,
  ROUND(
    SUM(sum_n_rows) OVER (ORDER BY year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(sum_expr_3) OVER (ORDER BY year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    (
      100.0 * (
        sum_expr_3 - LAG(sum_expr_3, 1) OVER (ORDER BY year_ca_dt)
      )
    ) / LAG(sum_expr_3, 1) OVER (ORDER BY year_ca_dt),
    2
  ) AS pct_bought_change,
  ROUND(
    (
      100.0 * (
        sum_n_rows - LAG(sum_n_rows, 1) OVER (ORDER BY year_ca_dt)
      )
    ) / LAG(sum_n_rows, 1) OVER (ORDER BY year_ca_dt),
    2
  ) AS pct_incident_change,
  sum_expr_3 AS bought,
  sum_n_rows AS incidents
FROM _t1
WHERE
  sum_expr_3 > 0
ORDER BY
  1 NULLS FIRST
