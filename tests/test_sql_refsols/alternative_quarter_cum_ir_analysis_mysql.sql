WITH _s0 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _t2 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.PRODUCTS
  WHERE
    pr_name = 'RubyCopper-Star'
), _s12 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s0.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s0.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter
  FROM _s0 AS _s0
  JOIN _t2 AS _t2
    ON _s0.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t2.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t2.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s0.ca_dt >= _t2.pr_release
), _t5 AS (
  SELECT
    pr_id,
    pr_name
  FROM main.PRODUCTS
  WHERE
    pr_name = 'RubyCopper-Star'
), _s9 AS (
  SELECT
    COUNTRIES.co_id,
    _t5.pr_id
  FROM _t5 AS _t5
  JOIN main.COUNTRIES AS COUNTRIES
    ON COUNTRIES.co_name = 'CN'
), _s13 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s2.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s2.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter,
    COUNT(DISTINCT INCIDENTS.in_device_id) AS ndistinct_in_device_id
  FROM _s0 AS _s2
  JOIN _t2 AS _t4
    ON _s2.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t4.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t4.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s2.ca_dt >= _t4.pr_release
  JOIN main.INCIDENTS AS INCIDENTS
    ON _s2.ca_dt = CAST(CAST(INCIDENTS.in_error_report_ts AS DATETIME) AS DATE)
  JOIN _s9 AS _s9
    ON INCIDENTS.in_repair_country_id = _s9.co_id
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id AND DEVICES.de_product_id = _s9.pr_id
  GROUP BY
    1
), _s21 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(
        YEAR(CAST(_s14.ca_dt AS DATETIME)),
        ' ',
        QUARTER(CAST(_s14.ca_dt AS DATETIME)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    ) AS quarter,
    COUNT(*) AS n_rows
  FROM _s0 AS _s14
  JOIN _t2 AS _t8
    ON _s14.ca_dt < STR_TO_DATE(
      CONCAT(
        YEAR(DATE_ADD(CAST(_t8.pr_release AS DATETIME), INTERVAL '2' YEAR)),
        ' ',
        QUARTER(DATE_ADD(CAST(_t8.pr_release AS DATETIME), INTERVAL '2' YEAR)) * 3 - 2,
        ' 1'
      ),
      '%Y %c %e'
    )
    AND _s14.ca_dt >= _t8.pr_release
  JOIN main.DEVICES AS DEVICES
    ON _s14.ca_dt = CAST(CAST(DEVICES.de_purchase_ts AS DATETIME) AS DATE)
  JOIN _t5 AS _t9
    ON DEVICES.de_product_id = _t9.pr_id
  GROUP BY
    1
)
SELECT
  _s12.quarter,
  _s13.ndistinct_in_device_id AS n_incidents,
  _s21.n_rows AS n_sold,
  ROUND(
    SUM(_s13.ndistinct_in_device_id) OVER (ORDER BY _s12.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(_s21.n_rows) OVER (ORDER BY _s12.quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS quarter_cum
FROM _s12 AS _s12
JOIN _s13 AS _s13
  ON _s12.quarter = _s13.quarter
JOIN _s21 AS _s21
  ON _s12.quarter = _s21.quarter
ORDER BY
  1
