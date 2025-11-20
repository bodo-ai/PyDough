WITH _t2 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.PRODUCTS
  WHERE
    pr_name = 'RubyCopper-Star'
), _s1 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _s7 AS (
  SELECT
    _s3.ca_dt,
    COUNT(*) AS n_rows
  FROM _t2 AS _t4
  JOIN _s1 AS _s3
    ON _s3.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t4.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t4.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s3.ca_dt >= _t4.pr_release
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_product_id = 800544
    AND _s3.ca_dt = CAST(CAST(DEVICES.de_purchase_ts AS DATETIME) AS DATE)
  GROUP BY
    1
), _s22 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s1.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s1.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter,
    SUM(_s7.n_rows) AS sum_n_rows
  FROM _t2 AS _t2
  JOIN _s1 AS _s1
    ON _s1.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t2.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t2.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s1.ca_dt >= _t2.pr_release
  LEFT JOIN _s7 AS _s7
    ON _s1.ca_dt = _s7.ca_dt
  GROUP BY
    1
), _s13 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s11.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s11.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter
  FROM _t2 AS _t10
  JOIN _s1 AS _s11
    ON _s11.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t10.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t10.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s11.ca_dt >= _t10.pr_release
), _s17 AS (
  SELECT
    _s15.ca_dt
  FROM _t2 AS _t11
  JOIN _s1 AS _s15
    ON _s15.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t11.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t11.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s15.ca_dt >= _t11.pr_release
), _s23 AS (
  SELECT
    _s13.quarter,
    COUNT(DISTINCT INCIDENTS.in_device_id) AS ndistinct_in_device_id
  FROM main.PRODUCTS AS PRODUCTS
  JOIN main.COUNTRIES AS COUNTRIES
    ON COUNTRIES.co_name = 'CN'
  CROSS JOIN _s13 AS _s13
  JOIN _s17 AS _s17
    ON _s13.quarter = STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s17.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s17.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
  JOIN main.INCIDENTS AS INCIDENTS
    ON COUNTRIES.co_id = INCIDENTS.in_repair_country_id
    AND _s17.ca_dt = CAST(CAST(INCIDENTS.in_error_report_ts AS DATETIME) AS DATE)
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id AND DEVICES.de_product_id = 800544
  WHERE
    PRODUCTS.pr_name = 'RubyCopper-Star'
  GROUP BY
    1
)
SELECT
  _s22.quarter,
  COALESCE(_s23.ndistinct_in_device_id, 0) AS n_incidents,
  COALESCE(_s22.sum_n_rows, 0) AS n_sold,
  ROUND(
    SUM(COALESCE(_s23.ndistinct_in_device_id, 0)) OVER (ORDER BY _s22.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s22.sum_n_rows, 0)) OVER (ORDER BY _s22.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM _s22 AS _s22
LEFT JOIN _s23 AS _s23
  ON _s22.quarter = _s23.quarter
ORDER BY
  1
