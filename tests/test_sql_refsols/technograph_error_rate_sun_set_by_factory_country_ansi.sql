WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s5 AS (
  SELECT
    SUM(COALESCE(_s3.agg_0, 0)) AS agg_0,
    COUNT() AS agg_1,
    devices.de_production_country_id AS factory_country_id
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
  ROUND(COALESCE(_s5.agg_0, 0) / COALESCE(_s5.agg_1, 0), 2) AS ir
FROM main.countries AS countries
LEFT JOIN _s5 AS _s5
  ON _s5.factory_country_id = countries.co_id
ORDER BY
  countries.co_name
