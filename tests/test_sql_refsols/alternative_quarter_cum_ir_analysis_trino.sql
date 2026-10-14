WITH _s2 AS (
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
), _t3 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _s16 AS (
  SELECT DISTINCT
    DATE_TRUNC('QUARTER', CAST(_s2.ca_dt AS TIMESTAMP)) AS quarter
  FROM _s2 AS _s2
  JOIN _t2 AS _t2
    ON _s2.ca_dt < DATE_TRUNC('QUARTER', DATE_ADD('YEAR', 2, CAST(_t2.pr_release AS TIMESTAMP)))
    AND _s2.ca_dt >= _t2.pr_release
  CROSS JOIN _t3 AS _t3
), _t7 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _s13 AS (
  SELECT
    countries.co_id,
    _t7.pr_id
  FROM _t7 AS _t7
  JOIN main.countries AS countries
    ON countries.co_name = 'CN'
), _s17 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(_s6.ca_dt AS TIMESTAMP)) AS quarter,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM _s2 AS _s6
  JOIN _t2 AS _t5
    ON _s6.ca_dt < DATE_TRUNC('QUARTER', DATE_ADD('YEAR', 2, CAST(_t5.pr_release AS TIMESTAMP)))
    AND _s6.ca_dt >= _t5.pr_release
  CROSS JOIN _t3 AS _t6
  JOIN main.incidents AS incidents
    ON _s6.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN _s13 AS _s13
    ON _s13.co_id = incidents.in_repair_country_id
  JOIN main.devices AS devices
    ON _s13.pr_id = devices.de_product_id AND devices.de_id = incidents.in_device_id
  GROUP BY
    1
), _s29 AS (
  SELECT
    DATE_TRUNC('QUARTER', CAST(_s20.ca_dt AS TIMESTAMP)) AS quarter,
    COUNT(*) AS n_rows
  FROM _s2 AS _s20
  JOIN _t2 AS _t10
    ON _s20.ca_dt < DATE_TRUNC('QUARTER', DATE_ADD('YEAR', 2, CAST(_t10.pr_release AS TIMESTAMP)))
    AND _s20.ca_dt >= _t10.pr_release
  CROSS JOIN _t3 AS _t11
  JOIN main.devices AS devices
    ON _s20.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t7 AS _t12
    ON _t12.pr_id = devices.de_product_id
  CROSS JOIN _t3 AS _t13
  GROUP BY
    1
)
SELECT
  _s16.quarter,
  COALESCE(_s17.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s29.n_rows, 0) AS n_sold,
  ROUND(
    CAST(SUM(COALESCE(_s17.ndistinct_in_device_id, 0)) OVER (ORDER BY _s16.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE) / SUM(COALESCE(_s29.n_rows, 0)) OVER (ORDER BY _s16.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM _s16 AS _s16
LEFT JOIN _s17 AS _s17
  ON _s16.quarter = _s17.quarter
LEFT JOIN _s29 AS _s29
  ON _s16.quarter = _s29.quarter
ORDER BY
  1 NULLS FIRST
