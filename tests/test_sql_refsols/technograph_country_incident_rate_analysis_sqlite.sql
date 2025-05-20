WITH _s1 AS (
  SELECT
    in_device_id AS device_id
  FROM main.incidents
), _s3 AS (
  SELECT
    COUNT() AS agg_0,
    devices.de_production_country_id AS factory_country_id
  FROM main.devices AS devices
  JOIN _s1 AS _s1
    ON _s1.device_id = devices.de_id
  GROUP BY
    devices.de_production_country_id
), _s5 AS (
  SELECT
    COUNT() AS agg_1,
    de_production_country_id AS factory_country_id
  FROM main.devices
  GROUP BY
    de_production_country_id
), _s9 AS (
  SELECT
    COUNT() AS agg_2,
    devices.de_purchase_country_id AS store_country_id
  FROM main.devices AS devices
  JOIN _s1 AS _s7
    ON _s7.device_id = devices.de_id
  GROUP BY
    devices.de_purchase_country_id
), _s11 AS (
  SELECT
    COUNT() AS agg_3,
    de_purchase_country_id AS store_country_id
  FROM main.devices
  GROUP BY
    de_purchase_country_id
), _s12 AS (
  SELECT
    us_id AS _id,
    us_country_id AS country_id
  FROM main.users
), _s17 AS (
  SELECT
    COUNT() AS agg_4,
    _s12.country_id
  FROM _s12 AS _s12
  JOIN main.devices AS devices
    ON _s12._id = devices.de_owner_id
  JOIN _s1 AS _s15
    ON _s15.device_id = devices.de_id
  GROUP BY
    _s12.country_id
), _s21 AS (
  SELECT
    COUNT() AS agg_5,
    _s18.country_id
  FROM _s12 AS _s18
  JOIN main.devices AS devices
    ON _s18._id = devices.de_owner_id
  GROUP BY
    _s18.country_id
)
SELECT
  countries.co_name AS country_name,
  ROUND(CAST(COALESCE(_s3.agg_0, 0) AS REAL) / _s5.agg_1, 2) AS made_ir,
  ROUND(CAST(COALESCE(_s9.agg_2, 0) AS REAL) / _s11.agg_3, 2) AS sold_ir,
  ROUND(CAST(COALESCE(_s17.agg_4, 0) AS REAL) / COALESCE(_s21.agg_5, 0), 2) AS user_ir
FROM main.countries AS countries
LEFT JOIN _s3 AS _s3
  ON _s3.factory_country_id = countries.co_id
JOIN _s5 AS _s5
  ON _s5.factory_country_id = countries.co_id
LEFT JOIN _s9 AS _s9
  ON _s9.store_country_id = countries.co_id
JOIN _s11 AS _s11
  ON _s11.store_country_id = countries.co_id
LEFT JOIN _s17 AS _s17
  ON _s17.country_id = countries.co_id
LEFT JOIN _s21 AS _s21
  ON _s21.country_id = countries.co_id
ORDER BY
  country_name
