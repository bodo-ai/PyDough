WITH _s0 AS (
  SELECT
    MAX(pr_release) AS anything_pr_release
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _t3 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s1 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%Y', ca_dt) AS INTEGER) AS year_ca_dt
  FROM _t3
), _t5 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s7 AS (
  SELECT
    CAST(STRFTIME('%Y', _s2.ca_dt) AS INTEGER) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t3 AS _s2
  JOIN main.devices AS devices
    ON _s2.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t5 AS _t5
    ON _t5.pr_id = devices.de_product_id
  GROUP BY
    1
), _s15 AS (
  SELECT
    CAST(STRFTIME('%Y', _s8.ca_dt) AS INTEGER) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t3 AS _s8
  JOIN main.incidents AS incidents
    ON _s8.ca_dt = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t5 AS _t7
    ON _t7.pr_id = devices.de_product_id
  GROUP BY
    1
)
SELECT
  _s1.year_ca_dt - CAST(STRFTIME('%Y', _s0.anything_pr_release) AS INTEGER) AS years_since_release,
  ROUND(
    CAST(SUM(COALESCE(_s15.n_rows, 0)) OVER (ORDER BY _s1.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(_s7.n_rows, 0)) OVER (ORDER BY _s1.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(_s7.n_rows, 0) - LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY _s1.year_ca_dt)
      )
    ) AS REAL) / LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY _s1.year_ca_dt),
    2
  ) AS pct_bought_change,
  ROUND(
    CAST((
      100.0 * (
        COALESCE(_s15.n_rows, 0) - LAG(COALESCE(_s15.n_rows, 0), 1) OVER (ORDER BY _s1.year_ca_dt)
      )
    ) AS REAL) / LAG(COALESCE(_s15.n_rows, 0), 1) OVER (ORDER BY _s1.year_ca_dt),
    2
  ) AS pct_incident_change,
  COALESCE(_s7.n_rows, 0) AS bought,
  COALESCE(_s15.n_rows, 0) AS incidents
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s1.year_ca_dt >= CAST(STRFTIME('%Y', _s0.anything_pr_release) AS INTEGER)
LEFT JOIN _s7 AS _s7
  ON _s1.year_ca_dt = _s7.year_ca_dt
LEFT JOIN _s15 AS _s15
  ON _s1.year_ca_dt = _s15.year_ca_dt
ORDER BY
  1
