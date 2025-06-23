WITH _t5 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    _s0.ca_dt AS calendar_day,
    COUNT(*) AS n_rows
  FROM _t5 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    _s0.ca_dt
), _s7 AS (
  SELECT
    _s4.ca_dt AS calendar_day,
    COUNT(*) AS n_rows
  FROM _t5 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    _s4.ca_dt
), _t3 AS (
  SELECT
    SUM(_s3.n_rows) AS sum_expr_3,
    SUM(_s7.n_rows) AS sum_n_rows,
    EXTRACT(YEAR FROM CAST(_t5.ca_dt AS DATETIME)) AS year
  FROM _t5 AS _t5
  LEFT JOIN _s3 AS _s3
    ON _s3.calendar_day = _t5.ca_dt
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = _t5.ca_dt
  GROUP BY
    EXTRACT(YEAR FROM CAST(_t5.ca_dt AS DATETIME))
), _t0 AS (
  SELECT
    ROUND(
      SUM(COALESCE(sum_n_rows, 0)) OVER (ORDER BY year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(sum_expr_3, 0)) OVER (ORDER BY year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    ROUND(
      (
        100.0 * (
          COALESCE(sum_expr_3, 0) - LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY year NULLS LAST)
        )
      ) / LAG(COALESCE(sum_expr_3, 0), 1) OVER (ORDER BY year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(sum_n_rows, 0) - LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY year NULLS LAST)
        )
      ) / LAG(COALESCE(sum_n_rows, 0), 1) OVER (ORDER BY year NULLS LAST),
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
