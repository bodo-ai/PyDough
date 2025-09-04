WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.INCIDENTS
  GROUP BY
    2
), _s5 AS (
  SELECT
    COALESCE(SUM(_s3.n_rows), 0) AS agg_0,
    DEVICES.de_production_country_id,
    COUNT(*) AS n_rows
  FROM main.DEVICES AS DEVICES
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'Sun-Set'
  LEFT JOIN _s3 AS _s3
    ON DEVICES.de_id = _s3.in_device_id
  GROUP BY
    2
)
SELECT
  COUNTRIES.co_name COLLATE utf8mb4_bin AS country,
  ROUND(COALESCE(_s5.agg_0, 0) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.COUNTRIES AS COUNTRIES
LEFT JOIN _s5 AS _s5
  ON COUNTRIES.co_id = _s5.de_production_country_id
ORDER BY
  1
