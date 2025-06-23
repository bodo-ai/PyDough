WITH _s3 AS (
  SELECT
    in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    in_device_id
), _s5 AS (
  SELECT
    devices.de_production_country_id AS factory_country_id,
    COUNT(*) AS n_rows,
    SUM(COALESCE(_s3.n_rows, 0)) AS sum_n_incidents
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  LEFT JOIN _s3 AS _s3
    ON _s3.device_id = devices.de_id
  GROUP BY
    devices.de_production_country_id
)
SELECT
  countries.co_name AS country,
  ROUND(CAST(COALESCE(_s5.sum_n_incidents, 0) AS REAL) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.countries AS countries
LEFT JOIN _s5 AS _s5
  ON _s5.factory_country_id = countries.co_id
ORDER BY
  countries.co_name
