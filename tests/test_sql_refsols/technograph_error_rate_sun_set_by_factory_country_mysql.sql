WITH _s3 AS (
  SELECT
    in_device_id
  FROM main.INCIDENTS
), _t1 AS (
  SELECT
    ANY_VALUE(DEVICES.de_production_country_id) AS anything_de_production_country_id,
    COUNT(_s3.in_device_id) AS count_in_device_id
  FROM main.DEVICES AS DEVICES
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'Sun-Set'
  LEFT JOIN _s3 AS _s3
    ON DEVICES.de_id = _s3.in_device_id
  GROUP BY
    DEVICES.de_id
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
  COUNTRIES.co_name COLLATE utf8mb4_bin AS country,
  ROUND(COALESCE(_s5.sum_count_in_device_id, 0) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.COUNTRIES AS COUNTRIES
LEFT JOIN _s5 AS _s5
  ON COUNTRIES.co_id = _s5.anything_de_production_country_id
ORDER BY
  1
