WITH _s3 AS (
  SELECT
    in_device_id
  FROM main.incidents
), _t1 AS (
  SELECT
    ANY_VALUE(devices.de_production_country_id) AS anything_de_production_country_id,
    COUNT(_s3.in_device_id) AS count_in_device_id
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  LEFT JOIN _s3 AS _s3
    ON _s3.in_device_id = devices.de_id
  GROUP BY
    devices.de_id
), _s5 AS (
  SELECT
    anything_de_production_country_id,
    COUNT(*) AS n_rows,
    SUM(count_in_device_id) AS sum_count_in_device_id
  FROM _t1
  GROUP BY
    1
)
SELECT
  countries.co_name AS country,
  ROUND(COALESCE(_s5.sum_count_in_device_id, 0) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.countries AS countries
LEFT JOIN _s5 AS _s5
  ON _s5.anything_de_production_country_id = countries.co_id
ORDER BY
  1
