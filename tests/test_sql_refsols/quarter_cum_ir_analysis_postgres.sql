WITH _t1 AS (
  SELECT
    pr_name
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _t3 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _s1 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s7 AS (
  SELECT
    _s3.ca_dt,
    COUNT(*) AS n_rows
  FROM _t3 AS _t5
  JOIN _s1 AS _s3
    ON _s3.ca_dt < DATE_TRUNC('QUARTER', CAST(_t5.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s3.ca_dt >= _t5.pr_release
  JOIN main.devices AS devices
    ON _s3.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
    AND devices.de_product_id = 800544
  GROUP BY
    1
), _s9 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(_s1.ca_dt AS TIMESTAMP)) AS quarter,
    SUM(_s7.n_rows) AS sum_n_rows
  FROM _t3 AS _t3
  JOIN _s1 AS _s1
    ON _s1.ca_dt < DATE_TRUNC('QUARTER', CAST(_t3.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s1.ca_dt >= _t3.pr_release
  LEFT JOIN _s7 AS _s7
    ON _s1.ca_dt = _s7.ca_dt
  GROUP BY
    1
), _s15 AS (
  SELECT DISTINCT
    DATE_TRUNC('QUARTER', CAST(_s13.ca_dt AS TIMESTAMP)) AS quarter
  FROM _t3 AS _t11
  JOIN _s1 AS _s13
    ON _s13.ca_dt < DATE_TRUNC('QUARTER', CAST(_t11.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s13.ca_dt >= _t11.pr_release
), _s19 AS (
  SELECT
    _s17.ca_dt
  FROM _t3 AS _t12
  JOIN _s1 AS _s17
    ON _s17.ca_dt < DATE_TRUNC('QUARTER', CAST(_t12.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s17.ca_dt >= _t12.pr_release
), _s25 AS (
  SELECT
    _s15.quarter,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM _t1 AS _t8
  JOIN main.countries AS countries
    ON countries.co_name = 'CN'
  CROSS JOIN _s15 AS _s15
  JOIN _s19 AS _s19
    ON _s15.quarter = DATE_TRUNC('QUARTER', CAST(_s19.ca_dt AS TIMESTAMP))
  JOIN main.incidents AS incidents
    ON _s19.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
    AND countries.co_id = incidents.in_repair_country_id
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id AND devices.de_product_id = 800544
  GROUP BY
    1
)
SELECT
  _s9.quarter,
  COALESCE(_s25.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s9.sum_n_rows, 0) AS n_sold,
  ROUND(
    CAST(CAST(SUM(COALESCE(_s25.ndistinct_in_device_id, 0)) OVER (ORDER BY _s9.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE PRECISION) / SUM(COALESCE(_s9.sum_n_rows, 0)) OVER (ORDER BY _s9.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL),
    2
  ) AS quarter_cum
FROM _t1 AS _t1
CROSS JOIN _s9 AS _s9
LEFT JOIN _s25 AS _s25
  ON _s25.quarter = _s9.quarter
ORDER BY
  1 NULLS FIRST
