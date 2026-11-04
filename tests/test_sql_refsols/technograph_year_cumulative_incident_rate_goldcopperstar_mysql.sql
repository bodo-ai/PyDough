WITH _s0 AS (
  SELECT
    ANY_VALUE(pr_release) AS anything_pr_release
  FROM main.PRODUCTS
  WHERE
    pr_name = 'GoldCopper-Star'
), _t3 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _s1 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM CAST(ca_dt AS DATETIME)) AS year_ca_dt
  FROM _t3
), _t5 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.PRODUCTS
  WHERE
    pr_name = 'GoldCopper-Star'
), _s7 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s2.ca_dt AS DATETIME)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t3 AS _s2
  JOIN main.DEVICES AS DEVICES
    ON _s2.ca_dt = CAST(CAST(DEVICES.de_purchase_ts AS DATETIME) AS DATE)
  JOIN _t5 AS _t5
    ON DEVICES.de_product_id = _t5.pr_id
  GROUP BY
    1
), _s15 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s8.ca_dt AS DATETIME)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _t3 AS _s8
  JOIN main.INCIDENTS AS INCIDENTS
    ON _s8.ca_dt = CAST(CAST(INCIDENTS.in_error_report_ts AS DATETIME) AS DATE)
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id
  JOIN _t5 AS _t7
    ON DEVICES.de_product_id = _t7.pr_id
  GROUP BY
    1
)
SELECT
  _s1.year_ca_dt - EXTRACT(YEAR FROM CAST(_s0.anything_pr_release AS DATETIME)) AS years_since_release,
  ROUND(
    SUM(COALESCE(_s15.n_rows, 0)) OVER (ORDER BY _s1.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s7.n_rows, 0)) OVER (ORDER BY _s1.year_ca_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS cum_ir,
  ROUND(
    (
      100.0 * (
        COALESCE(_s7.n_rows, 0) - LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY CASE WHEN _s1.year_ca_dt IS NULL THEN 1 ELSE 0 END, _s1.year_ca_dt)
      )
    ) / LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY CASE WHEN _s1.year_ca_dt IS NULL THEN 1 ELSE 0 END, _s1.year_ca_dt),
    2
  ) AS pct_bought_change,
  ROUND(
    (
      100.0 * (
        COALESCE(_s15.n_rows, 0) - LAG(COALESCE(_s15.n_rows, 0), 1) OVER (ORDER BY CASE WHEN _s1.year_ca_dt IS NULL THEN 1 ELSE 0 END, _s1.year_ca_dt)
      )
    ) / LAG(COALESCE(_s15.n_rows, 0), 1) OVER (ORDER BY CASE WHEN _s1.year_ca_dt IS NULL THEN 1 ELSE 0 END, _s1.year_ca_dt),
    2
  ) AS pct_incident_change,
  COALESCE(_s7.n_rows, 0) AS bought,
  COALESCE(_s15.n_rows, 0) AS incidents
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s1.year_ca_dt >= EXTRACT(YEAR FROM CAST(_s0.anything_pr_release AS DATETIME))
LEFT JOIN _s7 AS _s7
  ON _s1.year_ca_dt = _s7.year_ca_dt
LEFT JOIN _s15 AS _s15
  ON _s1.year_ca_dt = _s15.year_ca_dt
ORDER BY
  1
