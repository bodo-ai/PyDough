WITH _s0 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _t2 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _s12 AS (
  SELECT DISTINCT
    DATE_TRUNC('QUARTER', CAST(_s0.ca_dt AS TIMESTAMP)) AS quarter
  FROM _s0 AS _s0
  JOIN _t2 AS _t2
    ON _s0.ca_dt < DATE_TRUNC('QUARTER', CAST(_t2.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s0.ca_dt >= _t2.pr_release
), _t5 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _s9 AS (
  SELECT
    countries.co_id,
    _t5.pr_id
  FROM _t5 AS _t5
  JOIN main.countries AS countries
    ON countries.co_name = 'CN'
), _s13 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(_s2.ca_dt AS TIMESTAMP)) AS quarter,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM _s0 AS _s2
  JOIN _t2 AS _t4
    ON _s2.ca_dt < DATE_TRUNC('QUARTER', CAST(_t4.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s2.ca_dt >= _t4.pr_release
  JOIN main.incidents AS incidents
    ON _s2.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN _s9 AS _s9
    ON _s9.co_id = incidents.in_repair_country_id
  JOIN main.devices AS devices
    ON _s9.pr_id = devices.de_product_id AND devices.de_id = incidents.in_device_id
  GROUP BY
    1
), _s21 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(_s14.ca_dt AS TIMESTAMP)) AS quarter,
    COUNT(*) AS n_rows
  FROM _s0 AS _s14
  JOIN _t2 AS _t8
    ON _s14.ca_dt < DATE_TRUNC('QUARTER', CAST(_t8.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s14.ca_dt >= _t8.pr_release
  JOIN main.devices AS devices
    ON _s14.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t5 AS _t9
    ON _t9.pr_id = devices.de_product_id
  GROUP BY
    1
)
SELECT
  _s12.quarter,
  COALESCE(_s13.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s21.n_rows, 0) AS n_sold,
  ROUND(
    CAST(CAST(SUM(COALESCE(_s13.ndistinct_in_device_id, 0)) OVER (ORDER BY _s12.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE PRECISION) / SUM(COALESCE(_s21.n_rows, 0)) OVER (ORDER BY _s12.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL),
    2
  ) AS quarter_cum
FROM _s12 AS _s12
LEFT JOIN _s13 AS _s13
  ON _s12.quarter = _s13.quarter
LEFT JOIN _s21 AS _s21
  ON _s12.quarter = _s21.quarter
ORDER BY
  1 NULLS FIRST
