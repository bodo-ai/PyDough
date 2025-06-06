WITH _s7 AS (
  SELECT
    COUNT() AS agg_2,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s9 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(_s7.agg_2) AS agg_4,
    countries.co_id AS _id,
    countries_2.co_id AS _id_3
  FROM main.countries AS countries
  CROSS JOIN main.countries AS countries_2
  JOIN main.devices AS devices
    ON countries_2.co_id = devices.de_purchase_country_id
  LEFT JOIN _s7 AS _s7
    ON _s7.device_id = devices.de_id
  WHERE
    countries.co_id = devices.de_production_country_id
  GROUP BY
    countries.co_id,
    countries_2.co_id
)
SELECT
  countries.co_name AS factory_country,
  countries_2.co_name AS purchase_country,
  ROUND(CAST((
    1.0 * COALESCE(_s9.agg_4, 0)
  ) AS REAL) / COALESCE(_s9.agg_1, 0), 2) AS ir
FROM main.countries AS countries
CROSS JOIN main.countries AS countries_2
LEFT JOIN _s9 AS _s9
  ON _s9._id = countries.co_id AND _s9._id_3 = countries_2.co_id
ORDER BY
  ir DESC
LIMIT 5
