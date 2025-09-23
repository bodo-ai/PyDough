WITH _s3 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    1
), _s5 AS (
  SELECT
    devices.de_production_country_id,
    COUNT(*) AS n_rows,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  JOIN _s3 AS _s3
    ON _s3.in_device_id = devices.de_id
  GROUP BY
    1
)
SELECT
  countries.co_name AS country,
  ROUND(CAST(CAST(_s5.sum_n_rows AS DOUBLE PRECISION) / _s5.n_rows AS DECIMAL), 2) AS ir
FROM main.countries AS countries
JOIN _s5 AS _s5
  ON _s5.de_production_country_id = countries.co_id
ORDER BY
  1 NULLS FIRST
