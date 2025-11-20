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
    EXTRACT(YEAR FROM CAST(_s2.ca_dt AS TIMESTAMP)) AS year_cadt,
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
    CAST(CAST(SUM(COALESCE(sum_nrows, 0)) OVER (ORDER BY year_cadt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE PRECISION) / SUM(sum_expr3) OVER (ORDER BY year_cadt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL),
    2
  ) AS cum_ir,
  ROUND(
    CAST(CAST((
      100.0 * (
        sum_expr3 - LAG(sum_expr3, 1) OVER (ORDER BY year_cadt)
      )
    ) AS DOUBLE PRECISION) / LAG(sum_expr3, 1) OVER (ORDER BY year_cadt) AS DECIMAL),
    2
  ) AS pct_bought_change,
  ROUND(
    CAST(CAST((
      100.0 * (
        COALESCE(sum_nrows, 0) - LAG(COALESCE(sum_nrows, 0), 1) OVER (ORDER BY year_cadt)
      )
    ) AS DOUBLE PRECISION) / LAG(COALESCE(sum_nrows, 0), 1) OVER (ORDER BY year_cadt) AS DECIMAL),
    2
  ) AS pct_incident_change,
  sum_expr3 AS bought,
  COALESCE(sum_nrows, 0) AS incidents
FROM _t1
WHERE
  NOT sum_expr3 IS NULL AND sum_expr3 > 0
ORDER BY
  1 NULLS FIRST
