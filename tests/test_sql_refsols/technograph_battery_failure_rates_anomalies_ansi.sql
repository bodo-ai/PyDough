WITH _s7 AS (
  SELECT
    incidents.in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM main.incidents AS incidents
  JOIN main.errors AS errors
    ON errors.er_id = incidents.in_error_id AND errors.er_name = 'Battery Failure'
  GROUP BY
    incidents.in_device_id
), _t1 AS (
  SELECT
    countries.co_name AS country_name,
    COUNT(*) AS n_rows,
    products.pr_name AS product_name,
    SUM(COALESCE(_s7.n_rows, 0)) AS sum_n_incidents
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
  ROUND(COALESCE(sum_n_incidents, 0) / n_rows, 2) AS ir
FROM _t1
ORDER BY
  ir DESC,
  product_name,
  country_name
LIMIT 5
