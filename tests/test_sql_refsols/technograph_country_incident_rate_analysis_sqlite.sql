WITH _t3 AS (
  SELECT
    in_device_id
  FROM main.incidents
), _s1 AS (
  SELECT
    COUNT() AS agg_9,
    in_device_id AS device_id
  FROM _t3
  GROUP BY
    in_device_id
), _s3 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(_s1.agg_9) AS agg_11,
    devices.de_production_country_id AS factory_country_id
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1.device_id = devices.de_id
  GROUP BY
    devices.de_production_country_id
), _s5 AS (
  SELECT
    COUNT() AS agg_12,
    in_device_id AS device_id
  FROM _t3
  GROUP BY
    in_device_id
), _s7 AS (
  SELECT
    SUM(_s5.agg_12) AS agg_14,
    COUNT() AS agg_3,
    devices.de_purchase_country_id AS store_country_id
  FROM main.devices AS devices
  LEFT JOIN _s5 AS _s5
    ON _s5.device_id = devices.de_id
  GROUP BY
    devices.de_purchase_country_id
), _s11 AS (
  SELECT
    COUNT() AS agg_6,
    in_device_id AS device_id
  FROM _t3
  GROUP BY
    in_device_id
), _s13 AS (
  SELECT
    COUNT() AS agg_5,
    SUM(_s11.agg_6) AS agg_8,
    users.us_country_id AS country_id
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
  ROUND(CAST(COALESCE(_s3.agg_11, 0) AS REAL) / _s3.agg_1, 2) AS made_ir,
  ROUND(CAST(COALESCE(_s7.agg_14, 0) AS REAL) / _s7.agg_3, 2) AS sold_ir,
  ROUND(CAST(COALESCE(_s13.agg_8, 0) AS REAL) / COALESCE(_s13.agg_5, 0), 2) AS user_ir
FROM main.countries AS countries
JOIN _s3 AS _s3
  ON _s3.factory_country_id = countries.co_id
JOIN _s7 AS _s7
  ON _s7.store_country_id = countries.co_id
LEFT JOIN _s13 AS _s13
  ON _s13.country_id = countries.co_id
ORDER BY
  countries.co_name
