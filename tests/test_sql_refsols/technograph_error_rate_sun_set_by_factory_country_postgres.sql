WITH _s3 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    1
), _s5 AS (
  SELECT
    COALESCE(SUM(_s3.n_rows), 0) AS sum_nincidents,
    devices.de_production_country_id,
    COUNT(*) AS n_rows
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  LEFT JOIN _s3 AS _s3
    ON _s3.in_device_id = devices.de_id
  GROUP BY
    2
)
SELECT
  countries.co_name AS country,
  ROUND(
    CAST(CAST(COALESCE(_s5.sum_nincidents, 0) AS DOUBLE PRECISION) / COALESCE(_s5.n_rows, 0) AS DECIMAL),
    2
  ) AS ir
FROM main.countries AS countries
LEFT JOIN _s5 AS _s5
  ON _s5.de_production_country_id = countries.co_id
ORDER BY
  1 NULLS FIRST
