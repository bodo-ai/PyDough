WITH _t1 AS (
  SELECT
    co_name
  FROM main.COUNTRIES
  WHERE
    NOT co_name LIKE '%C%'
), _t2 AS (
  SELECT
    pr_name
  FROM main.PRODUCTS
  WHERE
    pr_name = 'AmethystCopper-I'
), _t5 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.PRODUCTS
  WHERE
    pr_name = 'AmethystCopper-I'
), _s5 AS (
  SELECT
    ca_dt
  FROM main.CALENDAR
), _s7 AS (
  SELECT DISTINCT
    STR_TO_DATE(CONCAT(YEAR(CAST(_s5.ca_dt AS DATETIME)), ' 1 1'), '%Y %c %e') AS start_of_year,
    _t4.co_name
  FROM _t1 AS _t4
  CROSS JOIN _t5 AS _t5
  JOIN _s5 AS _s5
    ON _s5.ca_dt < DATE_ADD(CAST(_t5.pr_release AS DATETIME), INTERVAL '2' YEAR)
    AND _s5.ca_dt >= _t5.pr_release
), _s15 AS (
  SELECT DISTINCT
    STR_TO_DATE(CONCAT(YEAR(CAST(_s13.ca_dt AS DATETIME)), ' 1 1'), '%Y %c %e') AS start_of_year,
    _t10.co_name
  FROM _t1 AS _t10
  CROSS JOIN _t5 AS _t11
  JOIN _s5 AS _s13
    ON _s13.ca_dt < DATE_ADD(CAST(_t11.pr_release AS DATETIME), INTERVAL '2' YEAR)
    AND _s13.ca_dt >= _t11.pr_release
), _s21 AS (
  SELECT
    _s19.ca_dt,
    _t12.co_name
  FROM _t1 AS _t12
  CROSS JOIN _t5 AS _t13
  JOIN _s5 AS _s19
    ON _s19.ca_dt < DATE_ADD(CAST(_t13.pr_release AS DATETIME), INTERVAL '2' YEAR)
    AND _s19.ca_dt >= _t13.pr_release
), _s29 AS (
  SELECT
    _t7.co_name,
    _s15.start_of_year,
    COUNT(*) AS n_rows
  FROM _t1 AS _t7
  CROSS JOIN _t2 AS _t8
  LEFT JOIN _s15 AS _s15
    ON _s15.co_name = _t7.co_name
  JOIN _s21 AS _s21
    ON _s15.start_of_year = STR_TO_DATE(CONCAT(YEAR(CAST(_s21.ca_dt AS DATETIME)), ' 1 1'), '%Y %c %e')
    AND _s21.co_name = _t7.co_name
  JOIN main.DEVICES AS DEVICES
    ON _s21.ca_dt = CAST(CAST(DEVICES.de_purchase_ts AS DATETIME) AS DATE)
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'AmethystCopper-I'
  JOIN main.COUNTRIES AS COUNTRIES
    ON COUNTRIES.co_id = DEVICES.de_purchase_country_id
    AND COUNTRIES.co_name = _t7.co_name
  GROUP BY
    1,
    2
)
SELECT
  _t1.co_name COLLATE utf8mb4_bin AS country_name,
  _s7.start_of_year,
  COALESCE(_s29.n_rows, 0) AS n_purchases
FROM _t1 AS _t1
CROSS JOIN _t2 AS _t2
LEFT JOIN _s7 AS _s7
  ON _s7.co_name = _t1.co_name
LEFT JOIN _s29 AS _s29
  ON _s29.co_name = _t1.co_name AND _s29.start_of_year = _s7.start_of_year
ORDER BY
  1,
  2
