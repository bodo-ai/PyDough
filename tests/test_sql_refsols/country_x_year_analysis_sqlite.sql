WITH _t1 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    NOT co_name LIKE '%C%'
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
), _s17 AS (
  SELECT
    _s9.ca_dt,
    _t7.co_name,
    COUNT(*) AS n_rows
  FROM _t1 AS _t7
  CROSS JOIN _t5 AS _t8
  JOIN _s5 AS _s9
    ON _s9.ca_dt < DATETIME(_t8.pr_release, '2 year') AND _s9.ca_dt >= _t8.pr_release
  JOIN main.devices AS devices
    ON _s9.ca_dt = DATE(devices.de_purchase_ts, 'start of day')
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'AmethystCopper-I'
  JOIN main.countries AS countries
    ON _t7.co_name = countries.co_name
    AND countries.co_id = devices.de_purchase_country_id
  GROUP BY
    1,
    2
), _s19 AS (
  SELECT
    DATE(_s5.ca_dt, 'start of year') AS start_of_year,
    _t4.co_name,
    SUM(_s17.n_rows) AS sum_n_rows
  FROM _t1 AS _t4
  CROSS JOIN _t5 AS _t5
  JOIN _s5 AS _s5
    ON _s5.ca_dt < DATETIME(_t5.pr_release, '2 year') AND _s5.ca_dt >= _t5.pr_release
  LEFT JOIN _s17 AS _s17
    ON _s17.ca_dt = _s5.ca_dt AND _s17.co_name = _t4.co_name
  GROUP BY
    1,
    2
)
SELECT
  _t1.co_name AS country_name,
  _s19.start_of_year,
  COALESCE(_s19.sum_n_rows, 0) AS n_purchases
FROM _t1 AS _t1
JOIN main.products AS products
  ON products.pr_name = 'AmethystCopper-I'
LEFT JOIN _s19 AS _s19
  ON _s19.co_name = _t1.co_name
ORDER BY
  1,
  2
