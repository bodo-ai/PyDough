WITH _t1 AS (
  SELECT
    pr_name
  FROM main.PRODUCTS
  WHERE
    pr_name = 'RubyCopper-Star'
), _t2 AS (
  SELECT
    co_name
  FROM main.COUNTRIES
  WHERE
    co_name = 'CN'
), _t4 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.PRODUCTS
  WHERE
    pr_name = 'RubyCopper-Star'
), _s5 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _s13 AS (
  SELECT
    _s9.ca_dt,
    COUNT(*) AS n_rows
  FROM _t4 AS _t7
  CROSS JOIN _t2 AS _t8
  JOIN _s5 AS _s9
    ON _s9.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t7.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t7.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s9.ca_dt >= _t7.pr_release
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_product_id = 800544
    AND _s9.ca_dt = CAST(CAST(DEVICES.de_purchase_ts AS DATETIME) AS DATE)
  GROUP BY
    1
), _s15 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s5.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s5.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter,
    SUM(_s13.n_rows) AS sum_n_rows
  FROM _t4 AS _t4
  CROSS JOIN _t2 AS _t5
  JOIN _s5 AS _s5
    ON _s5.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t4.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t4.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s5.ca_dt >= _t4.pr_release
  LEFT JOIN _s13 AS _s13
    ON _s13.ca_dt = _s5.ca_dt
  GROUP BY
    1
), _s23 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s21.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s21.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter
  FROM _t4 AS _t14
  CROSS JOIN _t2 AS _t15
  JOIN _s5 AS _s21
    ON _s21.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t14.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t14.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s21.ca_dt >= _t14.pr_release
), _s29 AS (
  SELECT
    _s27.ca_dt
  FROM _t4 AS _t16
  CROSS JOIN _t2 AS _t17
  JOIN _s5 AS _s27
    ON _s27.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t16.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t16.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s27.ca_dt >= _t16.pr_release
), _s35 AS (
  SELECT
    _s23.quarter,
    COUNT(DISTINCT INCIDENTS.in_device_id) AS ndistinct_in_device_id
  FROM _t1 AS _t11
  JOIN main.COUNTRIES AS COUNTRIES
    ON COUNTRIES.co_name = 'CN'
  CROSS JOIN _s23 AS _s23
  JOIN _s29 AS _s29
    ON _s23.quarter = STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s29.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s29.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
  JOIN main.INCIDENTS AS INCIDENTS
    ON COUNTRIES.co_id = INCIDENTS.in_repair_country_id
    AND _s29.ca_dt = CAST(CAST(INCIDENTS.in_error_report_ts AS DATETIME) AS DATE)
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id AND DEVICES.de_product_id = 800544
  GROUP BY
    1
)
SELECT
  _s15.quarter,
  COALESCE(_s35.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s15.sum_n_rows, 0) AS n_sold,
  ROUND(
    SUM(COALESCE(_s35.ndistinct_in_device_id, 0)) OVER (ORDER BY _s15.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s15.sum_n_rows, 0)) OVER (ORDER BY _s15.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM _t1 AS _t1
CROSS JOIN _t2 AS _t2
CROSS JOIN _s15 AS _s15
LEFT JOIN _s35 AS _s35
  ON _s15.quarter = _s35.quarter
ORDER BY
  1
