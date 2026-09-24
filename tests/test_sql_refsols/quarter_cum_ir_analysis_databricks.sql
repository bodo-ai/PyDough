WITH _t1 AS (
  SELECT
    pr_name
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _t2 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    co_name = 'CN'
), _t4 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'RubyCopper-Star'
), _s5 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s13 AS (
  SELECT
    _s9.ca_dt,
    COUNT(*) AS n_rows
  FROM _t4 AS _t7
  CROSS JOIN _t2 AS _t8
  JOIN _s5 AS _s9
    ON _s9.ca_dt < TRUNC(DATEADD(YEAR, 2, CAST(_t7.pr_release AS TIMESTAMP)), 'QUARTER')
    AND _s9.ca_dt >= _t7.pr_release
  JOIN main.devices AS devices
    ON _s9.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
    AND devices.de_product_id = 800544
  GROUP BY
    1
), _s15 AS (
  SELECT
    TRUNC(CAST(_s5.ca_dt AS TIMESTAMP), 'QUARTER') AS quarter,
    SUM(_s13.n_rows) AS sum_n_rows
  FROM _t4 AS _t4
  CROSS JOIN _t2 AS _t5
  JOIN _s5 AS _s5
    ON _s5.ca_dt < TRUNC(DATEADD(YEAR, 2, CAST(_t4.pr_release AS TIMESTAMP)), 'QUARTER')
    AND _s5.ca_dt >= _t4.pr_release
  LEFT JOIN _s13 AS _s13
    ON _s13.ca_dt = _s5.ca_dt
  GROUP BY
    1
), _s23 AS (
  SELECT DISTINCT
    TRUNC(CAST(_s21.ca_dt AS TIMESTAMP), 'QUARTER') AS quarter
  FROM _t4 AS _t14
  CROSS JOIN _t2 AS _t15
  JOIN _s5 AS _s21
    ON _s21.ca_dt < TRUNC(DATEADD(YEAR, 2, CAST(_t14.pr_release AS TIMESTAMP)), 'QUARTER')
    AND _s21.ca_dt >= _t14.pr_release
), _s29 AS (
  SELECT
    _s27.ca_dt
  FROM _t4 AS _t16
  CROSS JOIN _t2 AS _t17
  JOIN _s5 AS _s27
    ON _s27.ca_dt < TRUNC(DATEADD(YEAR, 2, CAST(_t16.pr_release AS TIMESTAMP)), 'QUARTER')
    AND _s27.ca_dt >= _t16.pr_release
), _s35 AS (
  SELECT
    _s23.quarter,
    COUNT(DISTINCT incidents.in_device_id) AS ndistinct_in_device_id
  FROM _t1 AS _t11
  JOIN main.countries AS countries
    ON countries.co_name = 'CN'
  CROSS JOIN _s23 AS _s23
  JOIN _s29 AS _s29
    ON _s23.quarter = TRUNC(CAST(_s29.ca_dt AS TIMESTAMP), 'QUARTER')
  JOIN main.incidents AS incidents
    ON _s29.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
    AND countries.co_id = incidents.in_repair_country_id
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id AND devices.de_product_id = 800544
  GROUP BY
    1
)
SELECT
  _s15.quarter,
  COALESCE(_s35.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s15.sum_n_rows, 0) AS n_sold,
  ROUND(
    SUM(COALESCE(_s35.ndistinct_in_device_id, 0)) OVER (ORDER BY _s15.quarter NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s15.sum_n_rows, 0)) OVER (ORDER BY _s15.quarter NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM _t1 AS _t1
CROSS JOIN _t2 AS _t2
CROSS JOIN _s15 AS _s15
LEFT JOIN _s35 AS _s35
  ON _s15.quarter = _s35.quarter
ORDER BY
  1
