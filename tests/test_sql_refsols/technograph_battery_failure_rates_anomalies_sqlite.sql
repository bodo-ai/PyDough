WITH _s7 AS (
  SELECT
    COUNT() AS agg_0,
    incidents.in_device_id AS device_id
  FROM main.incidents AS incidents
  JOIN main.errors AS errors
    ON errors.er_id = incidents.in_error_id AND errors.er_name = 'Battery Failure'
  GROUP BY
    incidents.in_device_id
), _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(COALESCE(_s7.agg_0, 0)) AS agg_0,
    countries.co_name AS country_name,
    products.pr_name AS product_name
  FROM main.countries AS countries
  JOIN main.devices AS devices
    ON countries.co_id = devices.de_production_country_id
  LEFT JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  LEFT JOIN _s7 AS _s7
    ON _s7.device_id = devices.de_id
  GROUP BY
    countries.co_name,
    products.pr_name
)
SELECT
  country_name,
  product_name,
  ROUND(CAST(COALESCE(agg_0, 0) AS REAL) / COALESCE(agg_1, 0), 2) AS ir
FROM _t1
ORDER BY
  ir DESC,
  product_name,
  country_name
LIMIT 5
