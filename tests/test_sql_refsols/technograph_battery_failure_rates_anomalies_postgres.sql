WITH _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    incidents.in_device_id
  FROM main.incidents AS incidents
  JOIN main.errors AS errors
    ON errors.er_id = incidents.in_error_id AND errors.er_name = 'Battery Failure'
  GROUP BY
    incidents.in_device_id
)
SELECT
  countries.co_name AS country_name,
  products.pr_name AS product_name,
  ROUND(CAST(COALESCE(SUM(_s7.n_rows), 0) AS DOUBLE PRECISION) / COUNT(*), 2) AS ir
FROM main.countries AS countries
JOIN main.devices AS devices
  ON countries.co_id = devices.de_production_country_id
JOIN main.products AS products
  ON devices.de_product_id = products.pr_id
LEFT JOIN _s7 AS _s7
  ON _s7.in_device_id = devices.de_id
GROUP BY
  countries.co_name,
  products.pr_name
ORDER BY
  ROUND(CAST(COALESCE(SUM(_s7.n_rows), 0) AS DOUBLE PRECISION) / COUNT(*), 2) DESC NULLS LAST,
  products.pr_name NULLS FIRST,
  countries.co_name NULLS FIRST
LIMIT 5
