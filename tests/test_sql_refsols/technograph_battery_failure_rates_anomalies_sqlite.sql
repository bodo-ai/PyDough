WITH _s4 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id,
    in_error_id AS error_id
  FROM main.incidents
  GROUP BY
    in_device_id,
    in_error_id
), _s7 AS (
  SELECT
    SUM(_s4.agg_0) AS agg_0,
    _s4.device_id
  FROM _s4 AS _s4
  JOIN main.errors AS errors
    ON _s4.error_id = errors.er_id AND errors.er_name = 'Battery Failure'
  GROUP BY
    _s4.device_id
), _t1 AS (
  SELECT
    SUM(COALESCE(_s7.agg_0, 0)) AS agg_0,
    COUNT() AS agg_1,
    countries.co_name AS country_name,
    products.pr_name AS product_name
  FROM main.countries AS countries
  JOIN main.devices AS devices
    ON countries.co_id = devices.de_production_country_id
  JOIN main.products AS products
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
  ROUND(CAST(COALESCE(agg_0, 0) AS REAL) / agg_1, 2) AS ir
FROM _t1
ORDER BY
  ir DESC,
  product_name,
  country_name
LIMIT 5
