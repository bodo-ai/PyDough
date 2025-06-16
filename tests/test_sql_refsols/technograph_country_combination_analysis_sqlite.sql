WITH _s0 AS (
  SELECT
    co_id
  FROM main.countries
), _s7 AS (
  SELECT
    COUNT() AS agg_2,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s9 AS (
  SELECT
    _s3.co_id AS _id_3,
    COUNT() AS agg_1,
    SUM(_s7.agg_2) AS agg_4,
    _s2.co_id AS _id
  FROM _s0 AS _s2
  CROSS JOIN _s0 AS _s3
  JOIN main.devices AS devices
    ON _s2.co_id = devices.de_production_country_id
    AND _s3.co_id = devices.de_purchase_country_id
  LEFT JOIN _s7 AS _s7
    ON _s7.device_id = devices.de_id
  GROUP BY
    _s3.co_id,
    _s2.co_id
)
SELECT
  countries.co_name AS factory_country,
  countries.co_name AS purchase_country,
  ROUND(CAST((
    1.0 * COALESCE(_s9.agg_4, 0)
  ) AS REAL) / COALESCE(_s9.agg_1, 0), 2) AS ir
FROM _s0 AS _s0
CROSS JOIN main.countries AS countries
LEFT JOIN _s9 AS _s9
  ON _s0.co_id = _s9._id AND _s9._id_3 = countries.co_id
ORDER BY
  ir DESC
LIMIT 5
