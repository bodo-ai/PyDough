WITH _t1 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    NOT CONTAINS(co_name, 'C')
), _t2 AS (
  SELECT
    pr_name
  FROM main.products
  WHERE
    pr_name = 'AmethystCopper-I'
), _t5 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'AmethystCopper-I'
), _s5 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s7 AS (
  SELECT DISTINCT
    TRUNC(CAST(_s5.ca_dt AS TIMESTAMP), 'YEAR') AS start_of_year,
    _t4.co_name
  FROM _t1 AS _t4
  CROSS JOIN _t5 AS _t5
  JOIN _s5 AS _s5
    ON _s5.ca_dt < DATE_ADD(YEAR, 2, CAST(_t5.pr_release AS TIMESTAMP))
    AND _s5.ca_dt >= _t5.pr_release
), _s15 AS (
  SELECT DISTINCT
    TRUNC(CAST(_s13.ca_dt AS TIMESTAMP), 'YEAR') AS start_of_year,
    _t10.co_name
  FROM _t1 AS _t10
  CROSS JOIN _t5 AS _t11
  JOIN _s5 AS _s13
    ON _s13.ca_dt < DATE_ADD(YEAR, 2, CAST(_t11.pr_release AS TIMESTAMP))
    AND _s13.ca_dt >= _t11.pr_release
), _s21 AS (
  SELECT
    _s19.ca_dt,
    _t12.co_name
  FROM _t1 AS _t12
  CROSS JOIN _t5 AS _t13
  JOIN _s5 AS _s19
    ON _s19.ca_dt < DATE_ADD(YEAR, 2, CAST(_t13.pr_release AS TIMESTAMP))
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
    ON _s15.start_of_year = TRUNC(CAST(_s21.ca_dt AS TIMESTAMP), 'YEAR')
    AND _s21.co_name = _t7.co_name
  JOIN main.devices AS devices
    ON _s21.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'AmethystCopper-I'
  JOIN main.countries AS countries
    ON _t7.co_name = countries.co_name
    AND countries.co_id = devices.de_purchase_country_id
  GROUP BY
    1,
    2
)
SELECT
  _t1.co_name AS country_name,
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
