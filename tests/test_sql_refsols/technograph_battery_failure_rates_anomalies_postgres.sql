WITH _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    incidents.in_device_id
  FROM main.incidents AS incidents
  JOIN main.errors AS errors
    ON errors.er_id = incidents.in_error_id AND errors.er_name = 'Battery Failure'
  GROUP BY
    2
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
  1,
  2
ORDER BY
  3 DESC NULLS LAST,
  2 NULLS FIRST,
  1 NULLS FIRST
LIMIT 5
