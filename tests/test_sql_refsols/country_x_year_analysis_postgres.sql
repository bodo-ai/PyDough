WITH _t1 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    NOT co_name LIKE '%C%'
), _t4 AS (
  SELECT
    pr_name,
    pr_release
  FROM main.products
  WHERE
    pr_name = 'AmethystCopper-I'
), _s3 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s15 AS (
  SELECT
    _s7.ca_dt,
    _t6.co_name,
    COUNT(*) AS n_rows
  FROM _t1 AS _t6
  CROSS JOIN _t4 AS _t7
  JOIN _s3 AS _s7
    ON _s7.ca_dt < CAST(_t7.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR'
    AND _s7.ca_dt >= _t7.pr_release
  JOIN main.devices AS devices
    ON _s7.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'AmethystCopper-I'
  JOIN main.countries AS countries
    ON _t6.co_name = countries.co_name
    AND countries.co_id = devices.de_purchase_country_id
  GROUP BY
    1,
    2
), _s17 AS (
  SELECT
    DATE_TRUNC('YEAR', CAST(_s3.ca_dt AS TIMESTAMP)) AS start_of_year,
    _t3.co_name,
    SUM(_s15.n_rows) AS sum_nrows
  FROM _t1 AS _t3
  CROSS JOIN _t4 AS _t4
  JOIN _s3 AS _s3
    ON _s3.ca_dt < CAST(_t4.pr_release AS TIMESTAMP) + INTERVAL '2 YEAR'
    AND _s3.ca_dt >= _t4.pr_release
  LEFT JOIN _s15 AS _s15
    ON _s15.ca_dt = _s3.ca_dt AND _s15.co_name = _t3.co_name
  GROUP BY
    1,
    2
)
SELECT
  _t1.co_name AS country_name,
  _s17.start_of_year,
  COALESCE(_s17.sum_nrows, 0) AS n_purchases
FROM _t1 AS _t1
LEFT JOIN _s17 AS _s17
  ON _s17.co_name = _t1.co_name
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
