WITH _t3 AS (
  SELECT
    in_device_id
  FROM main.incidents
), _s1 AS (
  SELECT
    in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    in_device_id
), _s3 AS (
  SELECT
    devices.de_production_country_id AS factory_country_id,
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_n_rows
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1.device_id = devices.de_id
  GROUP BY
    devices.de_production_country_id
), _s5 AS (
  SELECT
    in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    in_device_id
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    devices.de_purchase_country_id AS store_country_id,
    SUM(_s5.n_rows) AS sum_n_rows
  FROM main.devices AS devices
  LEFT JOIN _s5 AS _s5
    ON _s5.device_id = devices.de_id
  GROUP BY
    devices.de_purchase_country_id
), _s11 AS (
  SELECT
    in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    in_device_id
), _s13 AS (
  SELECT
    users.us_country_id AS country_id,
    COUNT(*) AS n_rows,
    SUM(_s11.n_rows) AS sum_n_rows
  FROM main.users AS users
  JOIN main.devices AS devices
    ON devices.de_owner_id = users.us_id
  LEFT JOIN _s11 AS _s11
    ON _s11.device_id = devices.de_id
  GROUP BY
    users.us_country_id
)
SELECT
  countries.co_name AS country_name,
  ROUND(COALESCE(_s3.sum_n_rows, 0) / _s3.n_rows, 2) AS made_ir,
  ROUND(COALESCE(_s7.sum_n_rows, 0) / _s7.n_rows, 2) AS sold_ir,
  ROUND(COALESCE(_s13.sum_n_rows, 0) / COALESCE(_s13.n_rows, 0), 2) AS user_ir
FROM main.countries AS countries
JOIN _s3 AS _s3
  ON _s3.factory_country_id = countries.co_id
JOIN _s7 AS _s7
  ON _s7.store_country_id = countries.co_id
LEFT JOIN _s13 AS _s13
  ON _s13.country_id = countries.co_id
ORDER BY
  countries.co_name
