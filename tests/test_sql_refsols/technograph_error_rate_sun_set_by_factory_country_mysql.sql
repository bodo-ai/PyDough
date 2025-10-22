WITH _t2 AS (
  SELECT
    ANY_VALUE(DEVICES.de_production_country_id) AS anything_de_production_country_id,
    COUNT(INCIDENTS.in_device_id) AS count_in_device_id
  FROM main.DEVICES AS DEVICES
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'Sun-Set'
  LEFT JOIN main.INCIDENTS AS INCIDENTS
    ON DEVICES.de_id = INCIDENTS.in_device_id
  GROUP BY
    DEVICES.de_id
), _s5 AS (
  SELECT
    COALESCE(SUM(CASE WHEN count_in_device_id > 0 THEN count_in_device_id ELSE NULL END), 0) AS sum_n_incidents,
    anything_de_production_country_id,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    2
)
SELECT
  COUNTRIES.co_name COLLATE utf8mb4_bin AS country,
  ROUND(COALESCE(_s5.sum_n_incidents, 0) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.COUNTRIES AS COUNTRIES
LEFT JOIN _s5 AS _s5
  ON COUNTRIES.co_id = _s5.anything_de_production_country_id
ORDER BY
  1
