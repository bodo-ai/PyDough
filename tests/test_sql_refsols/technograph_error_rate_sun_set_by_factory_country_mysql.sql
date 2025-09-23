WITH _s3 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.INCIDENTS
  GROUP BY
    1
), _s5 AS (
  SELECT
    DEVICES.de_production_country_id,
    COUNT(*) AS n_rows,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM main.DEVICES AS DEVICES
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'Sun-Set'
  JOIN _s3 AS _s3
    ON DEVICES.de_id = _s3.in_device_id
  GROUP BY
    1
)
SELECT
  COUNTRIES.co_name COLLATE utf8mb4_bin AS country,
  ROUND(_s5.sum_n_rows / _s5.n_rows, 2) AS ir
FROM main.COUNTRIES AS COUNTRIES
JOIN _s5 AS _s5
  ON COUNTRIES.co_id = _s5.de_production_country_id
ORDER BY
  1
