WITH _s13 AS (
  SELECT
    COUNT(*) AS agg_2,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s15 AS (
  SELECT
    COUNT(*) AS agg_1,
    SUM(_s13.agg_2) AS agg_4,
    _s4.co_id AS _id,
    _s5.co_id AS _id_3
  FROM main.countries AS _s4
  CROSS JOIN main.countries AS _s5
  JOIN main.devices AS _s8
    ON _s4.co_id = _s8.de_production_country_id
    AND _s5.co_id = _s8.de_purchase_country_id
  LEFT JOIN _s13 AS _s13
    ON _s13.device_id = _s8.de_id
  GROUP BY
    _s4.co_id,
    _s5.co_id
)
SELECT
  _s0.co_name AS factory_country,
  _s1.co_name AS purchase_country,
  ROUND(CAST((
    1.0 * COALESCE(_s15.agg_4, 0)
  ) AS REAL) / COALESCE(_s15.agg_1, 0), 2) AS ir
FROM main.countries AS _s0
CROSS JOIN main.countries AS _s1
LEFT JOIN _s15 AS _s15
  ON _s0.co_id = _s15._id AND _s1.co_id = _s15._id_3
ORDER BY
  ir DESC
LIMIT 5
