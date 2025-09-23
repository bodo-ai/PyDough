WITH _t2 AS (
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
  FROM _t2 AS _t4
  JOIN _s1 AS _s3
    ON _s3.ca_dt < DATE_TRUNC('QUARTER', CAST(_t4.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s3.ca_dt >= _t4.pr_release
  JOIN main.devices AS devices
    ON _s3.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
    AND devices.de_product_id = 800544
  GROUP BY
    1
), _s22 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(_s1.ca_dt AS TIMESTAMP)) AS quarter,
    SUM(_s7.n_rows) AS sum_n_rows
  FROM _t2 AS _t2
  JOIN _s1 AS _s1
    ON _s1.ca_dt < DATE_TRUNC('QUARTER', CAST(_t2.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s1.ca_dt >= _t2.pr_release
  JOIN _s7 AS _s7
    ON _s1.ca_dt = _s7.ca_dt
  GROUP BY
    1
), _s13 AS (
  SELECT DISTINCT
    DATE_TRUNC('QUARTER', CAST(_s11.ca_dt AS TIMESTAMP)) AS quarter
  FROM _t2 AS _t10
  JOIN _s1 AS _s11
    ON _s11.ca_dt < DATE_TRUNC('QUARTER', CAST(_t10.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s11.ca_dt >= _t10.pr_release
), _s17 AS (
  SELECT
    _s15.ca_dt
  FROM _t2 AS _t11
  JOIN _s1 AS _s15
    ON _s15.ca_dt < DATE_TRUNC('QUARTER', CAST(_t11.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR')
    AND _s15.ca_dt >= _t11.pr_release
), _s23 AS (
  SELECT
    _s13.quarter,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM main.products AS products
  JOIN main.countries AS countries
    ON countries.co_name = 'CN'
  CROSS JOIN _s13 AS _s13
  JOIN _s17 AS _s17
    ON _s13.quarter = DATE_TRUNC('QUARTER', CAST(_s17.ca_dt AS TIMESTAMP))
  JOIN main.incidents AS incidents
    ON _s17.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
    AND countries.co_id = incidents.in_repair_country_id
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id AND devices.de_product_id = 800544
  WHERE
    products.pr_name = 'RubyCopper-Star'
  GROUP BY
    1
)
SELECT
  _s22.quarter,
  _s23.ndistinct_in_device_id AS n_incidents,
  _s22.sum_n_rows AS n_sold,
  ROUND(
    CAST(CAST(SUM(_s23.ndistinct_in_device_id) OVER (ORDER BY _s22.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE PRECISION) / SUM(_s22.sum_n_rows) OVER (ORDER BY _s22.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL),
    2
  ) AS quarter_cum
FROM _s22 AS _s22
JOIN _s23 AS _s23
  ON _s22.quarter = _s23.quarter
ORDER BY
  1 NULLS FIRST
