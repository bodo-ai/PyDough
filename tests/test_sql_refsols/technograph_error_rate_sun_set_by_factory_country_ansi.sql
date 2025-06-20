WITH _s7 AS (
  SELECT
    COUNT(*) AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s9 AS (
  SELECT
    SUM(COALESCE(_s7.agg_0, 0)) AS agg_0,
    COUNT(*) AS agg_1,
    _s1.de_production_country_id AS factory_country_id
  FROM main.devices AS _s1
  JOIN main.products AS _s2
    ON _s1.de_product_id = _s2.pr_id AND _s2.pr_name = 'Sun-Set'
  LEFT JOIN _s7 AS _s7
    ON _s1.de_id = _s7.device_id
  GROUP BY
    _s1.de_production_country_id
)
SELECT
  _s0.co_name AS country,
  ROUND(COALESCE(_s9.agg_0, 0) / COALESCE(_s9.agg_1, 0), 2) AS ir
FROM main.countries AS _s0
LEFT JOIN _s9 AS _s9
  ON _s0.co_id = _s9.factory_country_id
ORDER BY
  country
