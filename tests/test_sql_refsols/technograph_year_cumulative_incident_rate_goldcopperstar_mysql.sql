WITH _s14 AS (
  SELECT
    ANY_VALUE(pr_release) AS release_date
  FROM main.PRODUCTS
  WHERE
    pr_name = 'GoldCopper-Star'
), _s6 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _t6 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.PRODUCTS
  WHERE
    pr_name = 'GoldCopper-Star'
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s0.ca_dt
  FROM _s6 AS _s0
  JOIN main.INCIDENTS AS INCIDENTS
    ON _s0.ca_dt = DATE(CAST(INCIDENTS.in_error_report_ts AS DATETIME))
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id
  JOIN _t6 AS _t6
    ON DEVICES.de_product_id = _t6.pr_id
  GROUP BY
    _s0.ca_dt
), _s13 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s8.ca_dt
  FROM _s6 AS _s8
  JOIN main.DEVICES AS DEVICES
    ON _s8.ca_dt = DATE(CAST(DEVICES.de_purchase_ts AS DATETIME))
  JOIN _t6 AS _t8
    ON DEVICES.de_product_id = _t8.pr_id
  GROUP BY
    _s8.ca_dt
), _s15 AS (
  SELECT
    SUM(_s7.n_rows) AS sum_expr_4,
    SUM(_s13.n_rows) AS sum_n_rows,
    YEAR(_s6.ca_dt) AS year
  FROM _s6 AS _s6
  LEFT JOIN _s7 AS _s7
    ON _s6.ca_dt = _s7.ca_dt
  LEFT JOIN _s13 AS _s13
    ON _s13.ca_dt = _s6.ca_dt
  GROUP BY
    YEAR(_s6.ca_dt)
)
SELECT
  _s15.year - YEAR(_s14.release_date) AS years_since_release,
  ROUND(
    SUM(COALESCE(_s15.sum_expr_4, 0)) OVER (ORDER BY _s15.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s15.sum_n_rows, 0)) OVER (ORDER BY _s15.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    (
      100.0 * (
        COALESCE(_s15.sum_n_rows, 0) - LAG(COALESCE(_s15.sum_n_rows, 0), 1) OVER (ORDER BY CASE WHEN _s15.year IS NULL THEN 1 ELSE 0 END, _s15.year)
      )
    ) / LAG(COALESCE(_s15.sum_n_rows, 0), 1) OVER (ORDER BY CASE WHEN _s15.year IS NULL THEN 1 ELSE 0 END, _s15.year),
    2
  ) AS pct_bought_change,
  ROUND(
    (
      100.0 * (
        COALESCE(_s15.sum_expr_4, 0) - LAG(COALESCE(_s15.sum_expr_4, 0), 1) OVER (ORDER BY CASE WHEN _s15.year IS NULL THEN 1 ELSE 0 END, _s15.year)
      )
    ) / LAG(COALESCE(_s15.sum_expr_4, 0), 1) OVER (ORDER BY CASE WHEN _s15.year IS NULL THEN 1 ELSE 0 END, _s15.year),
    2
  ) AS pct_incident_change,
  COALESCE(_s15.sum_n_rows, 0) AS bought,
  COALESCE(_s15.sum_expr_4, 0) AS incidents
FROM _s14 AS _s14
JOIN _s15 AS _s15
  ON _s15.year >= YEAR(_s14.release_date)
ORDER BY
  _s15.year - YEAR(_s14.release_date)
