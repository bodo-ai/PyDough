WITH _s2 AS (
  SELECT
    co_id AS _id,
    co_id AS factory_id
  FROM main.countries
), _s9 AS (
  SELECT
    COUNT() AS agg_0,
    _s2._id,
    countries.co_id AS _id_3
  FROM _s2 AS _s2
  CROSS JOIN main.countries AS countries
  JOIN main.devices AS devices
    ON countries.co_id = devices.de_purchase_country_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  WHERE
    _s2.factory_id = devices.de_production_country_id
  GROUP BY
    _s2._id,
    countries.co_id
), _s15 AS (
  SELECT
    COUNT() AS agg_1,
    _s10._id,
    countries.co_id AS _id_7
  FROM _s2 AS _s10
  CROSS JOIN main.countries AS countries
  JOIN main.devices AS devices
    ON countries.co_id = devices.de_purchase_country_id
  WHERE
    _s10.factory_id = devices.de_production_country_id
  GROUP BY
    _s10._id,
    countries.co_id
)
SELECT
  countries.co_name AS factory_country,
  countries_2.co_name AS purchase_country,
  ROUND((
    1.0 * COALESCE(_s9.agg_0, 0)
  ) / COALESCE(_s15.agg_1, 0), 2) AS ir
FROM main.countries AS countries
CROSS JOIN main.countries AS countries_2
LEFT JOIN _s9 AS _s9
  ON _s9._id = countries.co_id AND _s9._id_3 = countries_2.co_id
LEFT JOIN _s15 AS _s15
  ON _s15._id = countries.co_id AND _s15._id_7 = countries_2.co_id
ORDER BY
  ir DESC
LIMIT 5
