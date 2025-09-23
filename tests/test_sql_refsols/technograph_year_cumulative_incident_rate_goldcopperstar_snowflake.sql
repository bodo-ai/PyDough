WITH _s14 AS (
  SELECT
    ANY_VALUE(pr_release) AS anything_pr_release
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s6 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _t5 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s7 AS (
  SELECT
    _s0.ca_dt,
    COUNT(*) AS n_rows
  FROM _s6 AS _s0
  JOIN main.incidents AS incidents
    ON _s0.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t5 AS _t5
    ON _t5.pr_id = devices.de_product_id
  GROUP BY
    1
), _s13 AS (
  SELECT
    _s8.ca_dt,
    COUNT(*) AS n_rows
  FROM _s6 AS _s8
  JOIN main.devices AS devices
    ON _s8.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t5 AS _t7
    ON _t7.pr_id = devices.de_product_id
  GROUP BY
    1
), _s15 AS (
  SELECT
    YEAR(CAST(_s6.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    SUM(_s7.n_rows) AS sum_expr_4,
    SUM(_s13.n_rows) AS sum_n_rows
  FROM _s6 AS _s6
  JOIN _s7 AS _s7
    ON _s6.ca_dt = _s7.ca_dt
  JOIN _s13 AS _s13
    ON _s13.ca_dt = _s6.ca_dt
  GROUP BY
    1
)
SELECT
  _s15.year_ca_dt - YEAR(CAST(_s14.anything_pr_release AS TIMESTAMP)) AS years_since_release,
  ROUND(
    SUM(_s15.sum_expr_4) OVER (ORDER BY _s15.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(_s15.sum_n_rows) OVER (ORDER BY _s15.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    (
      100.0 * (
        _s15.sum_n_rows - LAG(_s15.sum_n_rows, 1) OVER (ORDER BY _s15.year_ca_dt)
      )
    ) / LAG(_s15.sum_n_rows, 1) OVER (ORDER BY _s15.year_ca_dt),
    2
  ) AS pct_bought_change,
  ROUND(
    (
      100.0 * (
        _s15.sum_expr_4 - LAG(_s15.sum_expr_4, 1) OVER (ORDER BY _s15.year_ca_dt)
      )
    ) / LAG(_s15.sum_expr_4, 1) OVER (ORDER BY _s15.year_ca_dt),
    2
  ) AS pct_incident_change,
  _s15.sum_n_rows AS bought,
  _s15.sum_expr_4 AS incidents
FROM _s14 AS _s14
JOIN _s15 AS _s15
  ON _s15.year_ca_dt >= YEAR(CAST(_s14.anything_pr_release AS TIMESTAMP))
ORDER BY
  1 NULLS FIRST
