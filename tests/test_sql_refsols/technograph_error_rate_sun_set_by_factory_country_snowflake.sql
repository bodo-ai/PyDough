WITH _t2 AS (
  SELECT
    ANY_VALUE(devices.de_production_country_id) AS anything_de_production_country_id,
    COUNT(incidents.in_device_id) AS count_in_device_id
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  LEFT JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    devices.de_id
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
  countries.co_name AS country,
  ROUND(COALESCE(_s5.sum_n_incidents, 0) / COALESCE(_s5.n_rows, 0), 2) AS ir
FROM main.countries AS countries
LEFT JOIN _s5 AS _s5
  ON _s5.anything_de_production_country_id = countries.co_id
ORDER BY
  1 NULLS FIRST
