WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.incidents
  GROUP BY
    2
), _s5 AS (
  SELECT
    COALESCE(SUM(_s3.n_rows), 0) AS agg_0,
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
  ROUND(COALESCE(_s5.agg_0, 0) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.countries AS countries
LEFT JOIN _s5 AS _s5
  ON _s5.de_production_country_id = countries.co_id
ORDER BY
  1 NULLS FIRST
