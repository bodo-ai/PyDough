WITH _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    INCIDENTS.in_device_id
  FROM main.INCIDENTS AS INCIDENTS
  JOIN main.ERRORS AS ERRORS
    ON ERRORS.er_id = INCIDENTS.in_error_id AND ERRORS.er_name = 'Battery Failure'
  GROUP BY
    2
)
SELECT
  COUNTRIES.co_name AS country_name,
  PRODUCTS.pr_name AS product_name,
  ROUND(COALESCE(SUM(_s7.n_rows), 0) / COUNT(*), 2) AS ir
FROM main.COUNTRIES AS COUNTRIES
JOIN main.DEVICES AS DEVICES
  ON COUNTRIES.co_id = DEVICES.de_production_country_id
JOIN main.PRODUCTS AS PRODUCTS
  ON DEVICES.de_product_id = PRODUCTS.pr_id
LEFT JOIN _s7 AS _s7
  ON DEVICES.de_id = _s7.in_device_id
GROUP BY
  1,
  2
ORDER BY
  3 DESC,
  2 COLLATE utf8mb4_bin,
  1 COLLATE utf8mb4_bin
LIMIT 5
